import argparse
import json
import os
import re
import itertools

import pandas as pd
import pyarrow as pa
import hashlib
import logging

from decimal import Decimal, InvalidOperation, getcontext
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Optional, List, Dict, Any
from decimal import localcontext, ROUND_HALF_UP


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

getcontext().prec = 2


@dataclass
class SebraSection:
    """
    Represents a section extracted from SEBRA xlsx file.

    * is_summary property tells if the object represents the summary section
      of the file or it represents a section for an organization.
    * The `data` DataFrame contains the extracted date. It structure is
      * when is_summary is True
        * start_date - DateTime
        * end_date - DateTime
        * operation_code - str
        * operation_description - str
        * currency - str, possible values are BGN or EUR
        * amount - Decimal
      * when is_summary is False
        * start_date - DateTime
        * end_date - DateTime
        * operation_code - str
        * operation_description - str
        * currency - str, possible values are BGN or EUR
        * amount - Decimal
        * organization_id - str
        * organization_name - str
    * The total_sum represents the total sum extracted from the end of the
      section in the file
    """
    is_summary: bool
    data: pd.DataFrame
    total_sum: Decimal

    def validate_total_sum(self) -> tuple[bool, Optional[str]]:
        """
        Validate that total_sum equals the sum of amounts in data DataFrame.

        Returns:
            tuple[bool, Optional[str]]: (True, None) if sums match exactly.
            (False, str) if they don't match, with description containing
            is_summary and the differing values.
        """
        if self.data.empty:
            calculated_sum = Decimal('0')
        else:
            # Use Decimal arithmetic with sufficient precision to avoid rounding
            # Save current context precision and use higher precision for sum
            with localcontext() as ctx:
                ctx.prec = 28  # Sufficient precision for monetary calculations
                ctx.rounding = ROUND_HALF_UP
                calculated_sum = Decimal('0')
                for amount in self.data['amount']:
                    if pd.notna(amount) and amount is not None:
                        calculated_sum += Decimal(str(amount))

        if self.total_sum == calculated_sum:
            return True, None

        if not self.is_summary:
            if not self.data.empty:
                org_id = self.data['organization_id'].iloc[0]
                org_name = self.data['organization_name'].iloc[0]
                org_info = f'{org_id=}, {org_name=}'
            else:
                org_info = 'the data frame is empty, org info cannot be extracted.'
        else:
            org_info = ''

        return False, (f"is_summary={self.is_summary}, "
                       f"total_sum={self.total_sum}, "
                       f"calculated_sum={calculated_sum},"
                       f' {org_info}')


@dataclass
class SebraData:
    """
    Represents all the sections extracted from a file

    * filename is the name of the file which was processed
    * summary contains the extracted data from the summary section
    * org_sections contains all the sections for organizations which were
      extracted
    """
    filename: str
    summary: SebraSection
    org_sections: list[SebraSection]

    def merged_org_data(self) -> pd.DataFrame:
        """
        Returns a DataFrame which is the concatenation of all org section dataframes.

        Returns:
            pd.DataFrame: concatenated DataFrame of all organization sections
        """
        if not self.org_sections:
            return pd.DataFrame(columns=[
                'start_date', 'end_date', 'operation_code',
                'operation_description', 'currency', 'amount',
                'organization_id', 'organization_name'
            ])
        return pd.concat([section.data for section in self.org_sections], ignore_index=True)

    def validate_total_sum(self) -> 'ValidationResult':
        """
        Validate total sums for all sections.

        Returns:
            ValidationResult: dataclass containing validation results and any errors
        """
        errors = []

        # Validate summary section
        summary_valid, summary_error = self.summary.validate_total_sum()
        if not summary_valid:
            errors.append(summary_error)

        # Validate all org sections
        sum_of_sums = Decimal(0)
        with localcontext() as ctx:
            ctx.prec = 28  # Sufficient precision for monetary calculations
            ctx.rounding = ROUND_HALF_UP

            sections_valid = True
            for section in self.org_sections:
                sum_of_sums += section.total_sum
                valid, error = section.validate_total_sum()
                if not valid:
                    errors.append(error)
                    sections_valid = False

        summary_sum_equals = sum_of_sums == self.summary.total_sum
        if not summary_sum_equals:
            errors.append(f'Total sum from summary item {self.summary.total_sum} differs from the sum of sums {sum_of_sums}')

            # Additional check: compare amounts by operation code
            merged_org_df = self.merged_org_data()
            if not self.summary.data.empty and not merged_org_df.empty:
                with localcontext() as ctx:
                    ctx.prec = 28
                    ctx.rounding = ROUND_HALF_UP

                    # For summary, each op_code appears once, so create Series directly
                    summary_by_op = self.summary.data.set_index('operation_code')['amount']
                    # For org data, group by operation_code and sum amounts
                    org_by_op = merged_org_df.groupby('operation_code')['amount'].apply(
                        lambda x: sum(Decimal(str(v)) for v in x if pd.notna(v) and v is not None)
                    )

                    # Find all operation codes
                    all_op_codes = set(summary_by_op.index) | set(org_by_op.index)

                    # Compare amounts for each operation code
                    mismatches = []
                    for op_code in sorted(all_op_codes):
                        summary_amt = summary_by_op.get(op_code, Decimal(0))
                        org_amt = org_by_op.get(op_code, Decimal(0))
                        if summary_amt != org_amt:
                            mismatches.append(f'op_code={op_code}: summary={summary_amt}, org_total={org_amt}')

                    if mismatches:
                        errors.append(f'Operation code amounts mismatch: {"; ".join(mismatches)}')

        is_valid = not errors

        return ValidationResult(
            filename=self.filename,
            is_valid=is_valid,
            is_summary_valid=summary_valid,
            are_sections_valid=sections_valid,
            is_summary_sum_equal_sum_of_sections=summary_sum_equals,
            errors=errors
        )


@dataclass
class ValidationResult:
    """
    Result of validating total sums in SebraData.

    * filename: the name of the file being validated
    * is_valid: True if all validations pass
    * is_summary_valid: True if the summary section's total matches its data
    * are_sections_valid: True if all organization sections' totals match their data
    * is_summary_sum_equal_sum_of_sections: True if summary total equals sum of all org section totals
    * errors: list of error messages (empty if is_valid is True)
    """
    filename: str
    is_valid: bool
    is_summary_valid: bool
    are_sections_valid: bool
    is_summary_sum_equal_sum_of_sections: bool
    errors: list[str]


def _parse_period(period_str: Any) -> tuple[Optional[str], Optional[str]]:
    """Extract start and end dates from period string like 'Период: 19.05.2023 - 19.05.2023'."""
    if not isinstance(period_str, str):
        return None, None
    m = re.search(r'(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})', period_str)
    if m:
        return m.group(1), m.group(2)
    return None, None


def _parse_org_header(header_str: Any) -> tuple[str, Optional[str]]:
    """Parse organization name and ID from string like 'Name ( ID )'."""
    if not isinstance(header_str, str):
        return '', None
    name = header_str.split('(', 1)[0].strip()
    m = re.search(r'\((.*?)\)', header_str)
    org_id = m.group(1).strip() if m else None
    return name, org_id


def _generate_code_from_text(text: str, length: int = 7) -> str:
    """Generate a code by uppercasing text, computing MD5, and taking first N chars."""
    return hashlib.md5(text.upper().encode()).hexdigest()[:length]


def _is_operation_code(val: Any) -> bool:
    """Check if string matches operation code pattern like '01 xxxx'."""
    if not isinstance(val, str):
        return False
    return bool(re.match(r'^\s*\d{2}\s*xxxx\s*$', val.strip(), flags=re.IGNORECASE))


def _is_descriptive_row(row: Any) -> bool:
    """Check if row has value only in first column (descriptive/skippable)."""
    if pd.isna(row[0]) or not str(row[0]).strip():
        return False
    return all(pd.isna(v) or not str(v).strip() for v in row[1:])

def _extract_currency(header_str: Any) -> Optional[str]:
    """Extract currency from summary header like 'ОБЩО ПЛАЩАНИЯ ЗА ДЕНЯ (в евро)'."""
    if not isinstance(header_str, str):
        return None
    m = re.search(r'\(в\s*(евро|лева)\)', header_str, flags=re.IGNORECASE)
    if m:
        currency_str = m.group(1).lower()
        if currency_str == 'евро':
            return 'EUR'
        elif currency_str == 'лева':
            return 'BGN'
    return None


def _is_summary_header(text: Any) -> bool:
    """Check if text is a summary section header."""
    if not isinstance(text, str):
        return False
    return 'ОБЩО ПЛАЩАНИЯ ЗА ДЕНЯ' in text


def _is_org_header(text: Any) -> bool:
    """Check if text looks like an organization header with parentheses."""
    if not isinstance(text, str):
        return False
    text = text.strip()
    # Organization headers typically have parentheses with ID
    return bool(re.search(r'\s*\(.*\)\s*$', text))


def _is_org_continuation_row(row: Any) -> bool:
    """Check if row is a continuation of org name (text only in first column, contains org ID, no period)."""
    if not isinstance(row[0], str) or not row[0].strip():
        return False
    # Check if first column contains org ID pattern (parentheses)
    if not bool(re.search(r'\(.*\)', row[0])):
        return False
    # Columns 1, 2, 3 should be empty/NaN
    return all(pd.isna(v) or not str(v).strip() for v in row[1:])


def _is_totals_row(text: Any) -> bool:
    """Check if text indicates a totals row."""
    if not isinstance(text, str):
        return False
    return 'Общо:' in text.strip()


def _is_na_or_empty(value: Any) -> bool:
    return (
        pd.isna(value) or
        pd.isnull(value) or
        (isinstance(value, str) and not value.strip())
    )


def _is_row_only_with_amount(row: Any) -> bool:
    """Check if text indicates a totals row."""

    return (
        (isinstance(row[3], float)) and
        _is_na_or_empty(row[0]) and
        _is_na_or_empty(row[1]) and
        _is_na_or_empty(row[2])
    )


def _is_header_row(row: Any) -> bool:
    """Check if row is a column header row (Код, Описание, Сума) or (Описание, Сума)."""
    if not isinstance(row[0], str):
        # Check for non-standard header with Описание in col1 and Сума in col3
        if (isinstance(row[1], str) and row[1].strip() == 'Описание' and
            isinstance(row[3], str) and row[3].strip() == 'Сума'):
            return True
        return False
    return (row[0].strip() == 'Код' and
            isinstance(row[1], str) and row[1].strip() == 'Описание' and
            isinstance(row[3], str) and row[3].strip() == 'Сума')


def _is_value_row(row: Any) -> bool:
    """
    Check if row has empty or str first column (the op code),
    string in the second column (the description) and float in the forth column
    (the amount).
    """
    return (
        (pd.isna(row[0]) or isinstance(row[0], str)) and
        (pd.isna(row[1]) or isinstance(row[1], str)) and
        (isinstance(row[3], float) or isinstance(row[3], int))
    )


def _parse_amount(value: Any) -> Optional[Decimal]:
    """Parse amount from cell value to Decimal with proper precision."""
    if pd.isna(value):
        return None
    try:
        # Format float to 2 decimal places to avoid precision issues
        if isinstance(value, float):
            value_str = '{:.2f}'.format(value)
        else:
            value_str = str(value).strip().replace(u'\xa0', '').replace(',', '.')
        return Decimal(value_str)
    except (InvalidOperation, ValueError, TypeError):
        logger.warning(f'Could not parse amount: {value}')
        return None


def _parse_operation_code(text: Any) -> Optional[str]:
    """Parse operation code from text, returns code or None."""
    if not isinstance(text, str):
        return None

    text = text.strip()

    # Check if it matches "01 xxxx" pattern
    if _is_operation_code(text):
        # Extract the code part (01)
        m = re.match(r'^\s*(\d{2})\s*xxxx\s*$', text, flags=re.IGNORECASE)
        if m:
            return m.group(1)

    # If it's just "xxxx" or similar without a code
    if text.lower().strip() == 'xxxx':
        return None

    # For descriptive text that doesn't match pattern
    return None


def _generate_operation_code_from_description(desc: Any) -> Optional[str]:
    """Generate operation code from description text."""
    if not isinstance(desc, str):
        return None
    return _generate_code_from_text(desc, length=7)


def _generate_org_id_from_name(name: Any) -> Optional[str]:
    """Generate organization ID from name."""
    if not isinstance(name, str):
        return None
    return _generate_code_from_text(name, length=10)


def log_to_xlsx_dir(func):
    @wraps(func)
    def wrapper(xlsx_path, *args, **kwargs):
        log_path = xlsx_path + '.log'
        if os.path.exists(log_path):
            os.unlink(log_path)
        logger = logging.getLogger(func.__module__)

        handler = logging.FileHandler(log_path, mode='a')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

        logger.addHandler(handler)
        logger.info(f"Starting {func.__name__} for {xlsx_path}")
        try:
            return func(xlsx_path, *args, **kwargs)
        finally:
            logger.removeHandler(handler)
    return wrapper


@log_to_xlsx_dir
def parse_sebra_payments_xlsx(xlsx_path: str) -> SebraData:
    """
    Parses a xlsx file with SEBRA payments and returns SebraData.

    Parsing steps:
    * after the xlsx file is loaded into a pd.DataFrame,
      all rows where all columns are NaN are dropped
    * the DataFrame should contain 4 columns, if not the process is interrupted with RuntimeException
    * the DataFrame is a sequence of multiple sections
    * at the very top there is a summary section which contains summarized amounts
      by operations codes
      * this section starts with "ОБЩО ПЛАЩАНИЯ ЗА ДЕНЯ (в евро)" or
        "ОБЩО ПЛАЩАНИЯ ЗА ДЕНЯ (в лева)" in the first column
        * the value within the brackets defines the currency
        * this currency type will be used for all the extracted data
        * if currency cannot be extracted the process is interrupted
          with RuntimeException
      * on the same row in the third column there's the period which looks like
        "Период: 19.05.2023 - 19.05.2023"
        * the two dates are in DD.MM.YYYY format
        * if the period cannot be extracted the process is interrupted with
          RuntimeException
      * this section may contain separating lines which provide additional
        descriptive row with text only in the first column,
        * these lines will be logged and skipped
      * a row within the summary section contains
        * in the first column an operation code in the form "01 xxxx"
          * there are operations without dedicated code and in this case
            this column contains "    xxxx"
          * for operations without dedicated operation code one will be
            calculated by making upper-casing the operation name and calculating its
            md5 sum. The final value will be first 7 chars from the md5 sum.
        * in the second column there's the operation code
        * the third column is empty
        * the forth column contains the amount
        * if a row does not match this pattern this should be logged
      * the section completes with a row which contains "Общо: " in the first row
        and total sum in the forth column;
        The value from the forth column should go to the total_sum property
        of the SebraSection object
    * The summary section might be followed by a descriptive row which has value
      only in the first column, this line is logged and be skipped
    * After that there could be one or more sections with payment operations, one
      section for each organization
      * Such a section starts with a row in which the first column looks like
        "Народно събрание ( 001******* )" where the text before the opening
        bracket is the organization name, while the text within the brackets
        is the organization id
        * In case there is no brackets section, then an organization ID will
          be calculated by upper-casing the contents of the first column and getting
          the first 10 chars of its md5 sum
      * In the third column there will be the period column which is in the same
        format as the one above Период: 19.05.2023 - 19.05.2023
      * After that there could be a row contain column headers like "Код", "Описание"
        and "Сума" in the first, second and forth columns - this row is logged and skipped
      * After that there are the operations rows; each row has
        * the operation code in the first column in the form "01 xxxx"
          if the code does not match the pattern, then the same approach for building
          a code as the one described above will be applied
        * the operation description in the second column
        * the amount in the forth column
      * A section for an organization completes with a row which contains
        "Общо: " in the first column and the total sum in the forth column
    """
    # Load the xlsx file
    path_obj = Path(xlsx_path)
    filename = path_obj.name

    logger.info(f'parsing {filename}')
    df = pd.read_excel(xlsx_path, header=None)

    # Drop rows where all columns are NaN
    df = df.dropna(how='all')

    # Check if DataFrame has exactly 4 columns
    if len(df.columns) != 4:
        raise RuntimeError(f"Expected 4 columns, got {len(df.columns)}")

    # Iterator over rows using iterrows() as specified
    row_iter = df.iterrows()

    # Get first non-empty row using next()
    _, first_row = next(row_iter)

    # Parse summary section header
    if not _is_summary_header(first_row[0]):
        raise RuntimeError(f"First row is not a summary header: {first_row[0]}")

    # Extract currency
    currency = _extract_currency(first_row[0])
    if currency is None:
        raise RuntimeError(f"Could not extract currency from: {first_row[0]}")

    # Extract period from third column (index 2)
    start_date, end_date = _parse_period(first_row[2])
    if start_date is None or end_date is None:
        raise RuntimeError(f"Could not extract period from: {first_row[2]}")

    # Now process summary section
    summary_rows = []
    summary_total = None

    # Process rows until we hit "Общо:" for summary
    for _, row in row_iter:
        # Check if this is the totals row
        if _is_totals_row(row[0]):
            summary_total = _parse_amount(row[3])
            if summary_total is None:
                logger.warning(f'[summary section] Could not parse summary total amount: {row[3]}, row={list(row)}')
                summary_total = Decimal('0')
            break

        # Skip descriptive rows (text only in first column)
        if _is_descriptive_row(row):
            logger.info(f'[summary section] Skipping descriptive row in summary: {row[0]}, row={list(row)}')
            continue

        # Skip column header rows
        if _is_header_row(row):
            logger.info(f'[summary section] Skipping column header row in summary, row={list(row)}')
            continue

        if _is_value_row(row):
            # Parse operation row
            op_code_raw = row[0]
            op_desc = row[1].strip() if isinstance(row[1], str) else ''

            # Parse amount
            amount = _parse_amount(row[3])
            if amount is None:
                logger.warning(f'[summary section] Skipping row with invalid amount: {row[3]}, row={list(row)}')
                continue

            # Parse operation code
            code = _parse_operation_code(op_code_raw)

            # If no code found (e.g., '    xxxx'), generate from description
            if code is None:
                code = _generate_operation_code_from_description(op_desc)
                if code is None:
                    logger.warning(f'[summary section] Could not generate operation code for: {op_desc}, row={list(row)}')
                    continue

            summary_rows.append({
                'start_date': start_date,
                'end_date': end_date,
                'operation_code': code,
                'operation_description': op_desc,
                'currency': currency,
                'amount': amount
            })
        else:
            row_types = [type(c) for c in row]
            logger.warning(f'[summary section] Skipping unexpected row: row={list(row)}, {row_types=}')

    # Create summary DataFrame
    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
    else:
        summary_df = pd.DataFrame(columns=[
            'start_date', 'end_date', 'operation_code',
            'operation_description', 'currency', 'amount'
        ])

    # Create summary section
    if summary_total is None:
        summary_total = Decimal('0')

    summary_section = SebraSection(
        is_summary=True,
        data=summary_df,
        total_sum=summary_total
    )

    # Now process organization sections
    org_sections = []

    # Some organization names span two rows like this:
    #   - on the first row in the first column is the first part of the name,
    #     the third column contains the period information
    #   - the next row contains the remaining part of the name where the org id
    #     is
    #
    # This fact requires sometimes to look ahead in the iterator.
    # That's why instead of simple for loop over the iterator we need to
    # walk it in while.
    while True:
        try:
            _, row = next(row_iter)
        except StopIteration:
            break

        # Check if this is a new organization section
        # An org section starts with a row that has:
        # - Non-empty first column (org name)
        # - A period string in the third column
        is_org_row = (isinstance(row[0], str) and row[0].strip() and
                     isinstance(row[2], str) and 'Период:' in row[2])

        if is_org_row or _is_org_header(row[0]):
            org_name, org_id = _parse_org_header(row[0])
            org_period_col = row[2]

            # Check for split org name across two rows
            # If no org_id found and this is an org_row (has period), check next row
            non_continuation_row = None
            if org_id is None and is_org_row:
                try:
                    next_idx, next_row = next(row_iter)
                    if _is_org_continuation_row(next_row):
                        # Combine with next row's first column
                        org_name_part2, org_id_part2 = _parse_org_header(next_row[0])
                        org_name = f"{org_name} {org_name_part2}".strip()
                        org_id = org_id_part2
                    else:
                        # Not a continuation, save it to process as first org row
                        non_continuation_row = (next_idx, next_row)
                except StopIteration:
                    pass

            # Extract period from third column
            org_start_date, org_end_date = _parse_period(org_period_col)

            # If no org_id from brackets or continuation, generate from name
            if org_id is None:
                org_id = _generate_org_id_from_name(org_name)

            org_rows = []
            org_total = None

            # If we have a non-continuation row, prepend it to the iterator
            org_row_iter = itertools.chain(
                [non_continuation_row] if non_continuation_row is not None else [],
                row_iter
            )

            # Process rows for this organization
            for _, org_row in org_row_iter:
                # Check if this is the totals row for this org
                if _is_totals_row(org_row[0]) or _is_row_only_with_amount(org_row):
                    org_total = _parse_amount(org_row[3])
                    if org_total is None:
                        logger.warning(f'[org section] Could not parse org total amount: {org_row[3]}, row={list(org_row)}')
                        org_total = Decimal('0')
                    break

                # Skip descriptive rows
                if _is_descriptive_row(org_row):
                    logger.info(f'[org section] Skipping descriptive row in org section: {org_row[0]}, row={list(org_row)}')
                    continue

                # Skip column header rows
                if _is_header_row(org_row):
                    logger.info(f'[org section] Skipping column header row in org section, row={list(org_row)}')
                    continue

                # Skip empty first column - but check if this might be an operation row
                # with code in col0 being empty
                if not isinstance(org_row[0], str) or not org_row[0].strip():
                    # Check if col1 has description and col3 has amount
                    # This is the non-standard format where code column is missing
                    if isinstance(org_row[1], str) and org_row[1].strip() and pd.notna(org_row[3]):
                        # This is an operation row without code in col0
                        op_code_raw = ''
                        op_desc = org_row[1].strip()
                    else:
                        # Truly empty, skip
                        logger.warning(f'[org section] Skipping truly empty row, row={list(org_row)}')
                        continue
                else:
                    op_code_raw = org_row[0].strip()
                    op_desc = org_row[1].strip() if isinstance(org_row[1], str) else ''

                # Parse amount
                amount = _parse_amount(org_row[3])
                if amount is None:
                    logger.warning(f'[org section] Skipping org row with invalid amount: {org_row[3]}, row={list(org_row)}')
                    continue

                # Parse operation code
                code = _parse_operation_code(op_code_raw)

                # If no code found, generate from description
                if code is None:
                    code = _generate_operation_code_from_description(op_desc)
                    if code is None:
                        logger.warning(f'[org section] Could not generate operation code for org: {op_desc}, row={list(org_row)}')
                        continue

                org_rows.append({
                    'start_date': org_start_date,
                    'end_date': org_end_date,
                    'operation_code': code,
                    'operation_description': op_desc,
                    'currency': currency,
                    'amount': amount,
                    'organization_id': org_id,
                    'organization_name': org_name
                })

            # Create org DataFrame
            if org_rows:
                org_df = pd.DataFrame(org_rows)
            else:
                org_df = pd.DataFrame(columns=[
                    'start_date', 'end_date', 'operation_code',
                    'operation_description', 'currency', 'amount',
                    'organization_id', 'organization_name'
                ])

            if org_total is None:
                logger.warning(f'[org section] Have not found totals row for organization {org_name}.')
                org_total = Decimal('0')

            org_section = SebraSection(
                is_summary=False,
                data=org_df,
                total_sum=org_total
            )
            org_sections.append(org_section)
        elif _is_descriptive_row(row):
            # Descriptive row between sections, skip it
            logger.info(f'[org section] Skipping descriptive row between sections: {row[0]}, row={list(row)}')
            continue
        elif _is_summary_header(row[0]):
            # This shouldn't happen, but skip
            logger.warning(f'[org section] Unexpected summary header in middle of file: {row[0]}, row={list(row)}')
            continue
        else:
            # Unexpected row, log and skip
            logger.warning(f'[org section] Unexpected row, row={list(row)}')
            continue

    # Create and return SebraData
    return SebraData(
        filename=filename,
        summary=summary_section,
        org_sections=org_sections
    )


def _cmd_parse_and_validate(args: argparse.Namespace) -> None:
    """Handle the parse-and-validate command."""
    sebra_data = parse_sebra_payments_xlsx(args.filepath)
    validation_result = sebra_data.validate_total_sum()
    print(json.dumps(validation_result.__dict__))


def main() -> None:
    """Main entry point for the ETL CLI."""
    parser = argparse.ArgumentParser(description='SEBRA ETL tool')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # parse-and-validate command
    parse_parser = subparsers.add_parser(
        'parse-and-validate',
        help='Parse xlsx file and validate its contents'
    )
    parse_parser.add_argument(
        'filepath',
        type=str,
        help='Path to xlsx file to parse and validate'
    )

    args = parser.parse_args()

    if args.command == 'parse-and-validate':
        _cmd_parse_and_validate(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()