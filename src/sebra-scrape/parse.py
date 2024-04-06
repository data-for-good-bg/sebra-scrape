import numpy as np
import pandas as pd


class SebraReportParsingException(Exception):
    """
    Raised when parsing fails for any reason. The reason should be mentioned explicitly in the error message
    """

    pass


def get_general_totals(df, general_op_block):
    new_columns = [
        "Operations Code",
        "Operations Description",
        "Operations Count",
        "Operations Amount (BGN)",
    ]
    b_start, b_end = general_op_block
    block_df = df.iloc[b_start + 1 : b_end].copy()
    block_df.columns = new_columns
    del block_df["Operations Count"]

    # drop all rows that do not have amount value
    block_df = block_df[~block_df["Operations Amount (BGN)"].isna()]

    block_df["Operations Amount (BGN)"] = (
        block_df["Operations Amount (BGN)"]
        .astype(str)
        .str.replace(" ", "")
        .str.replace(",", ".")
        .astype("float64")
    )
    block_df = block_df.set_index("Operations Code")

    # check if sums are correct
    sum_row = df.iloc[b_end, :]
    sum_row.index = new_columns
    if isinstance(sum_row["Operations Amount (BGN)"], str):
        sum_row["Operations Amount (BGN)"] = float(
            sum_row["Operations Amount (BGN)"].replace(" ", "").replace(",", ".")
        )
    if not round(sum_row["Operations Amount (BGN)"], 2) == round(
        block_df["Operations Amount (BGN)"].sum(), 2
    ):
        raise SebraReportParsingException(
            f'The sums of "Operations Amount (BGN)" do not match for block ({b_start}, {b_end}). Expected value '
            f"\"{round(sum_row['Operations Amount (BGN)'], 2)}\". Calculated value "
            f"\"{round(block_df['Operations Amount (BGN)'].sum(), 2)}\"."
        )

    return block_df


def get_organization_name_and_period(df, b_start):
    block_header = df.iloc[b_start - 1]
    org_name = block_header.iloc[0]

    if isinstance(block_header.iloc[2], str):
        # this is the standard case - the header is just one row
        period = block_header.iloc[2].split(" ")
    else:
        # this is a rare case where the header takes two rows
        block_header_1 = df.iloc[b_start - 2]
        # concat the organization name along the two rows

        if not isinstance(block_header_1[0], str):
            raise SebraReportParsingException(
                f"The first column does not contain the organization name around row {b_start}."
            )

        org_name = block_header_1[0] + " " + org_name
        if not isinstance(block_header_1[2], str):
            raise SebraReportParsingException(
                f"The third column does not contain the period string around row {b_start}."
            )
        period = block_header_1[2].split(" ")
    if period[0] != "Период:":
        raise SebraReportParsingException(
            f'Field for time period should start with string "Период". Around row {b_start}.'
        )

    start_date = period[1]
    end_date = period[3]

    return org_name, start_date, end_date


def get_organization_operations_blocks(df, org_op_blocks):
    new_columns = [
        "Operations Code",
        "Operations Description",
        "Operations Count",
        "Operations Amount (BGN)",
    ]
    org_op_blocks_dfs = []
    for b_start, b_end in org_op_blocks:
        org_df = df.iloc[b_start + 1 : b_end, :].copy()
        if org_df.iloc[:, 1].isna().any():
            raise SebraReportParsingException(
                f"There are empty cells within the operations block between rows ({b_start}, {b_end})."
            )
        org_df.columns = new_columns
        del org_df["Operations Count"]
        org_df["Operations Amount (BGN)"] = (
            org_df["Operations Amount (BGN)"]
            .astype(str)
            .str.replace(" ", "")
            .str.replace(",", ".")
            .astype("float64")
        )

        # get organization name and time period
        org_name, start_date, end_date = get_organization_name_and_period(df, b_start)
        org_df["Organization Name"] = org_name
        org_df["Start Date"] = start_date
        org_df["End Date"] = end_date

        # check if sums are correct
        sum_row = df.iloc[b_end, :]
        sum_row.index = new_columns
        if isinstance(sum_row["Operations Amount (BGN)"], str):
            sum_row["Operations Amount (BGN)"] = float(
                sum_row["Operations Amount (BGN)"].replace(" ", "").replace(",", ".")
            )
        if not round(sum_row["Operations Amount (BGN)"], 2) == round(
            org_df["Operations Amount (BGN)"].sum(), 2
        ):
            # TODO: Just warning?
            raise SebraReportParsingException(
                f'The sums of "Operations Amount (BGN)" do not match for block ({b_start}, {b_end}).'
                f"Expected value \"{round(sum_row['Operations Amount (BGN)'], 2)}\". Calculated value "
                f"\"{round(org_df['Operations Amount (BGN)'].sum(), 2)}\"."
            )

        org_op_blocks_dfs.append(org_df)
    return pd.concat(org_op_blocks_dfs).reset_index().drop(columns=["index"])


def check_sums(ops_df, general_block_df):
    ops_totals_df = ops_df.groupby("Operations Code").sum()
    if not (ops_df["Operations Code"].dtypes == object) and (
        general_block_df.reset_index()["Operations Code"].dtypes == object
    ):
        raise SebraReportParsingException("There are missing operation codes.")

    sum_check_df = pd.merge(
        ops_totals_df,
        general_block_df,
        how="left",
        left_on="Operations Code",
        right_index=True,
    )
    if not (
        sum_check_df["Operations Amount (BGN)_x"].round(2)
        == sum_check_df["Operations Amount (BGN)_y"].round(2)
    ).all():
        # TODO: Just warn?
        raise SebraReportParsingException(
            "Some sums do not match Operations Amount (BGN)."
        )


def find_organizations_start_row(df):
    org_start_phrases = [
        "ПЛАЩАНИЯ ПО ПЪРВОСТЕПЕННИ СИСТЕМИ В СЕБРА ",
        "ПЛАЩАНИЯ ОТ БЮДЖЕТА, ИЗВЪРШЕНИ ЧРЕЗ СЕБРА, ПО ПЪРВОСТЕПЕННИ СИСТЕМИ",
    ]
    correct = False
    for t in org_start_phrases:
        mask = df.iloc[:, 0] == t
        if mask.sum() == 1:
            correct = True
            break

    if not correct:
        raise SebraReportParsingException(
            f"There should be exactly one row containing any of the phrases: {org_start_phrases}"
        )

    return mask[mask].index[0]


def find_operations_blocks(df, org_start_row):
    op_start_text = "Описание"
    op_end_text = "Общо:"
    mask_start = df.iloc[:, 1] == op_start_text
    mask_end = df.iloc[:, 0].str.startswith(op_end_text).fillna(False)
    op_start_index = mask_start[mask_start].index
    op_end_index = mask_end[mask_end].index

    # TODO: why is this here? maybe need to warn/error?
    if not (len(op_start_index) == len(op_end_index)):
        print(op_start_index, op_end_index)

    op_blocks = list(zip(op_start_index, op_end_index))
    op_blocks_dict = {"general": [], "organizations": []}
    for i in range(len(op_blocks)):
        b_start, b_end = op_blocks[i]
        if i > 0:
            _, prev_b_end = op_blocks[i - 1]
            if not prev_b_end < b_start:
                raise SebraReportParsingException(
                    f"Block end index ({prev_b_end}) of the previous block should be smaller than the start index of "
                    f"the next block ({b_start})."
                )
        if b_start >= b_end:
            raise SebraReportParsingException(
                f"Block start index ({b_start}) should be smaller than the end index ({b_end})."
            )
        if b_start < org_start_row and org_start_row < b_end:
            raise SebraReportParsingException(
                "The starting row for organizations should not be within an operations block."
            )
        if b_end < org_start_row:
            op_blocks_dict["general"].append((b_start, b_end))
        else:
            op_blocks_dict["organizations"].append((b_start, b_end))

    if len(op_blocks_dict["general"]) != 1:
        raise SebraReportParsingException(
            "There should be exactly one block for general totals."
        )

    return op_blocks_dict


def parse_sebra_report(df):
    # get the row that indicates the start of the organizations section
    org_start_row = find_organizations_start_row(df)

    # get the start and end rows for all blocks of operations
    op_blocks = find_operations_blocks(df, org_start_row)

    # parse the data containing the general totals for all operations in the file
    # (we will use those just to check the sums at the end for correctness)
    general_block_df = get_general_totals(df, op_blocks["general"][0])

    # parse all operations by organization in the file
    ops_df = get_organization_operations_blocks(df, op_blocks["organizations"])

    # check if the sums for all opearions by organization match the general totals
    check_sums(ops_df, general_block_df)

    # extract the Organization ID from the Name
    ops_df["Organization ID"] = (
        ops_df["Organization Name"]
        .str.findall(r"\((.*?)\)")
        .map(lambda x: x[-1].strip() if len(x) > 0 else np.nan)
    )

    return ops_df


if __name__ == "__main__":
    df_raw = pd.read_excel("downloaded/raw/sebra/SEBRA-16032023.xlsx")
    df_parsed = parse_sebra_report(df_raw)
    df_parsed.to_csv("parsed_new.csv")
    print(df_parsed)
