import os
import re
import shutil

import pandas as pd
import pyarrow as pa
import hashlib
import re
import logging

from decimal import Decimal, InvalidOperation, getcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any



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



def _parse_period(period_str):
    """Extract start and end dates from period string like 'Период: 19.05.2023 - 19.05.2023'."""
    if not isinstance(period_str, str):
        return None, None
    m = re.search(r'(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})', period_str)
    if m:
        return m.group(1), m.group(2)
    return None, None


def _parse_org_header(header_str):
    """Parse organization name and ID from string like 'Name ( ID )'."""
    if not isinstance(header_str, str):
        return '', None
    name = header_str.split('(', 1)[0].strip()
    m = re.search(r'\((.*?)\)', header_str)
    org_id = m.group(1).strip() if m else None
    return name, org_id


def _generate_code_from_text(text, length=7):
    """Generate a code by uppercasing text, computing MD5, and taking first N chars."""
    return hashlib.md5(text.upper().encode()).hexdigest()[:length]


def _is_operation_code(val):
    """Check if string matches operation code pattern like '01 xxxx'."""
    if not isinstance(val, str):
        return False
    return bool(re.match(r'^\s*\d{2}\s*xxxx\s*$', val.strip(), flags=re.IGNORECASE))


def _is_descriptive_row(row):
    """Check if row has value only in first column (descriptive/skippable)."""
    if pd.isna(row[0]) or not str(row[0]).strip():
        return False
    return all(pd.isna(v) or not str(v).strip() for v in row[1:])

def _round(v):
    if isinstance(v, float):
        return round(v, 2)
    else:
        return v


def _extract_currency(header_str):
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


def _is_summary_header(text):
    """Check if text is a summary section header."""
    if not isinstance(text, str):
        return False
    return 'ОБЩО ПЛАЩАНИЯ ЗА ДЕНЯ' in text


def _is_org_header(text):
    """Check if text looks like an organization header with parentheses."""
    if not isinstance(text, str):
        return False
    text = text.strip()
    # Organization headers typically have parentheses with ID
    return bool(re.search(r'\s*\(.*\)\s*$', text))


def _is_totals_row(text):
    """Check if text indicates a totals row."""
    if not isinstance(text, str):
        return False
    return 'Общо:' in text.strip()


def _is_header_row(row):
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


def _parse_amount(value):
    """Parse amount from cell value to Decimal with proper precision."""
    if pd.isna(value):
        return None
    try:
        # Format float to 2 decimal places to avoid precision issues
        if isinstance(value, float):
            value_str = '{:.2f}'.format(value)
        else:
            value_str = str(value).strip().replace(',', '.')
        return Decimal(value_str)
    except (InvalidOperation, ValueError, TypeError):
        logger.warning(f"Could not parse amount: {value}")
        return None


def _parse_operation_code(text):
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


def _generate_operation_code_from_description(desc):
    """Generate operation code from description text."""
    if not isinstance(desc, str):
        return None
    return _generate_code_from_text(desc, length=7)


def _generate_org_id_from_name(name):
    """Generate organization ID from name."""
    if not isinstance(name, str):
        return None
    return _generate_code_from_text(name, length=10)


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
                logger.warning(f"Could not parse summary total amount: {row[3]}")
                summary_total = Decimal('0')
            break

        # Skip descriptive rows (text only in first column)
        if _is_descriptive_row(row):
            logger.info(f"Skipping descriptive row in summary: {row[0]}")
            continue

        # Skip column header rows
        if _is_header_row(row):
            logger.info(f"Skipping column header row in summary")
            continue

        # Skip empty first column
        if not isinstance(row[0], str) or not row[0].strip():
            continue

        # Parse operation row
        op_code_raw = row[0].strip()
        op_desc = row[1].strip() if isinstance(row[1], str) else ''

        # Parse amount
        amount = _parse_amount(row[3])
        if amount is None:
            logger.warning(f"Skipping row with invalid amount: {row[3]}")
            continue

        # Parse operation code
        code = _parse_operation_code(op_code_raw)

        # If no code found (e.g., "    xxxx"), generate from description
        if code is None:
            code = _generate_operation_code_from_description(op_desc)
            if code is None:
                logger.warning(f"Could not generate operation code for: {op_desc}")
                continue

        summary_rows.append({
            'start_date': start_date,
            'end_date': end_date,
            'operation_code': code,
            'operation_description': op_desc,
            'currency': currency,
            'amount': amount
        })

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

    for _, row in row_iter:
        # Check if this is a new organization section
        # An org section starts with a row that has:
        # - Non-empty first column (org name)
        # - A period string in the third column
        is_org_row = (isinstance(row[0], str) and row[0].strip() and
                     isinstance(row[2], str) and 'Период:' in row[2])

        if is_org_row or _is_org_header(row[0]):
            org_name, org_id = _parse_org_header(row[0])

            # Extract period from third column
            org_start_date, org_end_date = _parse_period(row[2])

            # If no org_id from brackets, generate from name
            if org_id is None:
                org_id = _generate_org_id_from_name(org_name)

            org_rows = []
            org_total = None

            # Process rows for this organization
            for _, org_row in row_iter:
                # Check if this is the totals row for this org
                if _is_totals_row(org_row[0]):
                    org_total = _parse_amount(org_row[3])
                    if org_total is None:
                        logger.warning(f"Could not parse org total amount: {org_row[3]}")
                        org_total = Decimal('0')
                    break

                # Skip descriptive rows
                if _is_descriptive_row(org_row):
                    logger.info(f"Skipping descriptive row in org section: {org_row[0]}")
                    continue

                # Skip column header rows
                if _is_header_row(org_row):
                    logger.info(f"Skipping column header row in org section")
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
                        continue
                else:
                    op_code_raw = org_row[0].strip()
                    op_desc = org_row[1].strip() if isinstance(org_row[1], str) else ''

                # Parse amount
                amount = _parse_amount(org_row[3])
                if amount is None:
                    logger.warning(f"Skipping org row with invalid amount: {org_row[3]}")
                    continue

                # Parse operation code
                code = _parse_operation_code(op_code_raw)

                # If no code found, generate from description
                if code is None:
                    code = _generate_operation_code_from_description(op_desc)
                    if code is None:
                        logger.warning(f"Could not generate operation code for org: {op_desc}")
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
                org_total = Decimal('0')

            org_section = SebraSection(
                is_summary=False,
                data=org_df,
                total_sum=org_total
            )
            org_sections.append(org_section)
        elif _is_descriptive_row(row):
            # Descriptive row between sections, skip it
            logger.info(f"Skipping descriptive row between sections: {row[0]}")
            continue
        elif _is_summary_header(row[0]):
            # This shouldn't happen, but skip
            logger.warning(f"Unexpected summary header in middle of file: {row[0]}")
            continue
        else:
            # Unexpected row, log and skip
            logger.warning(f"Unexpected row: {row}")
            continue

    # Create and return SebraData
    return SebraData(
        filename=filename,
        summary=summary_section,
        org_sections=org_sections
    )