import os
import pandas as pd
import numpy as np

class SebraParser:
    def __init__(self, parent_folder):
        self.parent_folder = parent_folder

    def get_all_excel_files(self):
        data_dir = self.parent_folder
        folders = [f for f in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, f))]
        excel_files = []
        for fold in folders:
            if int(fold) < 2019:
                continue
            excel_files += [
                os.path.join(data_dir, fold, f) \
                    for f in os.listdir(os.path.join(data_dir, fold)) \
                        if os.path.isfile(os.path.join(data_dir, fold, f)) \
                            and (f.endswith('.xlsx') or f.endswith('.xls'))
            ]
        return sorted(excel_files)

    def read_into_df(self, excel_file_path):
        self.df = pd.read_excel(excel_file_path)

    # def parse_sebra_excel_file_to_pandas(self):
    def find_organizations_start_row(self, excel_file_path):
        org_start_phrases = [
            'ПЛАЩАНИЯ ПО ПЪРВОСТЕПЕННИ СИСТЕМИ В СЕБРА ',
            'ПЛАЩАНИЯ ОТ БЮДЖЕТА, ИЗВЪРШЕНИ ЧРЕЗ СЕБРА, ПО ПЪРВОСТЕПЕННИ СИСТЕМИ'
        ]
        correct = False
        for t in org_start_phrases:
            mask = self.df.iloc[:, 0] == t
            if mask.sum() == 1:
                correct = True
                break
        assert correct, f'There should be exactly one row containing any of the phrases {org_start_phrases}. File "{excel_file_path}".'
        return mask[mask].index[0]
    
    def find_operations_blocks(self, org_start_row, excel_file_path):
        op_start_text = 'Описание'
        op_end_text = 'Общо:'
        mask_start = self.df.iloc[:, 1] == op_start_text
        mask_end = self.df.iloc[:, 0].str.startswith(op_end_text).fillna(False)
        op_start_index = mask_start[mask_start].index
        op_end_index = mask_end[mask_end].index
        
        if not (len(op_start_index) == len(op_end_index)):
            print(op_start_index, op_end_index)
            # display(df)
            
        assert len(op_start_index) == len(op_end_index), f'The number of occurances of "{op_start_text}" is not the same as the number of occurances of "{op_end_text}". File "{excel_file_path}".'
        op_blocks = list(zip(op_start_index, op_end_index))
        op_blocks_dict = {
            'general': [],
            'organizations': []
        }
        for i in range(len(op_blocks)):
            b_start, b_end = op_blocks[i]
            assert b_start < b_end, f'Block start index ({b_start}) should be smaller that the end index ({b_end}). File "{excel_file_path}".'
            if i > 0:
                _, prev_b_end = op_blocks[i - 1]
                assert prev_b_end < b_start, f'Block end index ({prev_b_end}) of the previous block should be smaller that the start index of the next block ({b_start}). File "{excel_file_path}".'
            assert not (b_start < org_start_row and org_start_row < b_end), f'The starting row for organizations shouldn\'t be within an operations block. File "{excel_file_path}".'

            if b_end < org_start_row:
                op_blocks_dict['general'].append((b_start, b_end))
            else:
                op_blocks_dict['organizations'].append((b_start, b_end))

            assert len(op_blocks_dict['general']) == 1, 'There should be exactly one block for general totals. File "{excel_file_path}".'

        return op_blocks_dict

    def get_general_totals(self, general_op_block, excel_file_path):
        new_columns = ['Operations Code', 'Operations Description', 'Operations Count', 'Operations Amount (BGN)']
        b_start, b_end = general_op_block
        block_df = self.df.iloc[b_start + 1: b_end].copy()
        block_df.columns = new_columns
        del block_df['Operations Count']

        # drop all rows that do not have amount value
        block_df = block_df[~block_df['Operations Amount (BGN)'].isna()]

        block_df['Operations Amount (BGN)'] = block_df['Operations Amount (BGN)'] \
            .astype(str) \
            .str.replace(' ', '') \
            .str.replace(',', '.') \
            .astype('float64')
        block_df = block_df.set_index('Operations Code')

        # check if sums are correct
        sum_row = self.df.iloc[b_end, :]
        sum_row.index = new_columns
        if isinstance(sum_row['Operations Amount (BGN)'], str):
            sum_row['Operations Amount (BGN)'] = float(
                sum_row['Operations Amount (BGN)'] \
                    .replace(' ', '') \
                    .replace(',', '.')
            )
        if not round(sum_row['Operations Amount (BGN)'], 2) == round(block_df['Operations Amount (BGN)'].sum(), 2):
            print(f"Warning: The sums of \"Operations Amount (BGN)\" do not match for block ({b_start}, {b_end}). Expected value \"{round(sum_row['Operations Amount (BGN)'], 2)}\". Calculated value \"{round(block_df['Operations Amount (BGN)'].sum(), 2)}\". File \"{excel_file_path}\".")

        return block_df

    def get_organization_name_and__period(self, b_start, excel_file_path):
        block_header = self.df.iloc[b_start - 1]
        org_name = block_header[0]
        if isinstance(block_header[2], str):
            # this is the standard case - the header is just one row
            period = block_header[2].split(' ')
        else:
            # this is a rare case where the header takes two rows
            block_header_1 = self.df.iloc[b_start - 2]
            # concat the organization name along the two rows
            assert isinstance(block_header_1[0], str), f'The first column does not contain the organization name around row {b_start} in file "{excel_file_path}".'
            org_name = block_header_1[0] + ' ' + org_name
            assert isinstance(block_header_1[2], str), f'The third column does not contain the period string around row {b_start} in file "{excel_file_path}".'
            period = block_header_1[2].split(' ')
        assert period[0] == 'Период:', f'Field for time period should start with string "Период". File "{excel_file_path}".'
        start_date = period[1]
        end_date = period[3]
        return org_name, start_date, end_date

    def get_organization_operations_blocks(self, org_op_blocks, excel_file_path):
        
    
        new_columns = ['Operations Code', 'Operations Description', 'Operations Count', 'Operations Amount (BGN)']
        org_op_blocks_dfs = []
        for b_start, b_end in org_op_blocks:
            org_df = self.df.iloc[b_start + 1: b_end, :].copy()
            assert not org_df.iloc[:, 1].isna().any(), f'There are empty cells within the operations block between rows ({b_start}, {b_end}). File "{excel_file_path}".'
            org_df.columns = new_columns
            del org_df['Operations Count']
            org_df['Operations Amount (BGN)'] = org_df['Operations Amount (BGN)'] \
                .astype(str) \
                .str.replace(' ', '') \
                .str.replace(',', '.') \
                .astype('float64')

            # get organization name and time period
            org_name, start_date, end_date = self.get_organization_name_and__period(b_start, excel_file_path)
            org_df['Organization Name'] = org_name
            org_df['Start Date'] = start_date
            org_df['End Date'] = end_date

            # check if sums are correct
            sum_row = self.df.iloc[b_end, :]
            sum_row.index = new_columns
            if isinstance(sum_row['Operations Amount (BGN)'], str):
                sum_row['Operations Amount (BGN)'] = float(
                    sum_row['Operations Amount (BGN)'] \
                        .replace(' ', '') \
                        .replace(',', '.')
                )
            if not round(sum_row['Operations Amount (BGN)'], 2) == round(org_df['Operations Amount (BGN)'].sum(), 2):
                print(f"Warning: The sums of \"Operations Amount (BGN)\" do not match for block ({b_start}, {b_end}). Expected value \"{round(sum_row['Operations Amount (BGN)'], 2)}\". Calculated value \"{round(org_df['Operations Amount (BGN)'].sum(), 2)}\". File \"{excel_file_path}\".")

            org_op_blocks_dfs.append(org_df)
        return pd.concat(org_op_blocks_dfs).reset_index().drop(columns=['index'])

    def check_sums(self, ops_df, general_block_df, excel_file_path):
        ops_totals_df = ops_df.groupby('Operations Code').sum()
        
        assert (ops_df['Operations Code'].dtypes == object) and (general_block_df.reset_index()['Operations Code'].dtypes == object), f'There are missing operation codes in file "{excel_file_path}".'
        
        sum_check_df = pd.merge(
            ops_totals_df,
            general_block_df,
            how='left',
            left_on='Operations Code',
            right_index=True
        )
        if not (sum_check_df['Operations Amount (BGN)_x'].round(2) == sum_check_df['Operations Amount (BGN)_y'].round(2)).all():
            print(f'Warning: Some sums do not match Operations Amount (BGN). File "{excel_file_path}".')

    def run_parser(self):
        # Get all excel files
        excel_files = self.get_all_excel_files()

        dfs = []
        for i, f in enumerate(excel_files):
            print(i, f)
            try:
            # Read data
                self.read_into_df(f)

                # get the row that indicates the start of the organizations section
                org_start_row = self.find_organizations_start_row(f)
                
                # get the start and end rows for all blocks of operations
                op_blocks = self.find_operations_blocks(org_start_row, f)
                
                # parse the data containing the general totals for all operations in the file
                # (we will use those just to check the sums at the end for correctness)
                general_block_df = self.get_general_totals(op_blocks['general'][0], f)
                
                # parse all operations by organization in the file
                ops_df = self.get_organization_operations_blocks(op_blocks['organizations'], f)
                
                # check if the sums for all opearions by organization match the general totals
                self.check_sums(ops_df, general_block_df, f)

                # extract the Organization ID from the Name
                ops_df['Organization ID'] = ops_df['Organization Name'].str.findall(r'\((.*?)\)').map(
                    lambda x: x[-1].strip() if len(x) > 0 else np.nan
                )
            except:
                ops_df = None

            dfs.append(ops_df)
        
        df = pd.concat(dfs)
        return df
