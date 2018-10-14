import os
import re
import pandas as pd
from collections import Counter

min_column_count = 3
min_row_count = 4


def read_file_as_string(path):
    output = ''
    with open(path, 'r') as content:
        output = content.read()
    return output


class TableData:
    def __init__(self, file_name, dual_headers):
        self.dual_headers = dual_headers

        self.text_line_data = []
        self.big_spaces_pos_list = []

        # headers info
        self.headers = []
        self.headers_char_position = []
        self.headers_line_position = []

        # data body
        self.data_body = []
        self.dataframe = []

        self.process_file(file_name)
        return

    def extract_array_data_from_big_spaces_pos(self, line_position):
        line_data = self.text_line_data[line_position]
        big_spaces_pos = self.big_spaces_pos_list[line_position]

        inverse_pos = self.get_inverse_pos(line_data, big_spaces_pos)
        return [line_data[x[0]:x[1] + 1] for x in inverse_pos]

    def process_file(self, file_path):
        self.text_line_data = read_file_as_string(file_path).split('\n')

        big_spaces_pos = self.big_spaces_pos_list
        for line in self.text_line_data:
            l1_regex = re.compile(r'  +').finditer(line)
            big_spaces_pos.append([x.span() for x in l1_regex])

        big_spaces_counts = [len(x) for x in big_spaces_pos]
        big_spaces_counts_filtered = list(filter(lambda x: x >= min_column_count, big_spaces_counts))
        big_spaces_counts_frequency = Counter(big_spaces_counts_filtered)

        # dict to array
        big_spaces_counts_frequency = [[x, big_spaces_counts_frequency[x]] for x in big_spaces_counts_frequency]
        big_spaces_counts_frequency.sort(key=lambda x: x[1], reverse=True)

        # assume actual table column count by highest reasonable frequency
        column_count = big_spaces_counts_frequency[0][0]
        if column_count < min_row_count:
            print('Cannot find table with at least ', min_row_count, ' rows.')

        empty_rows_counter = 0
        for i in range(len(big_spaces_pos)):
            if len(re.findall(r'[a-zA-Z0-9]', self.text_line_data[i])) == 0:
                empty_rows_counter += 1
                continue

            if big_spaces_counts[i] == column_count:
                if not self.headers:
                    empty_rows_counter = 0
                    self.headers = self.extract_array_data_from_big_spaces_pos(i)
                    self.headers_char_position = self.get_inverse_pos(self.text_line_data[i], big_spaces_pos[i])
                    self.headers_line_position = i
                else:
                    # adds normal table rows
                    empty_rows_counter = 0  # reset
                    self.data_body.append(
                        self.get_headers_overlapped_values(i))


            # handles rows where col < table col
            elif self.headers:
                def helper_x(row, empty_rows_counter):
                    temp = self.get_headers_overlapped_values(i)
                    if all(e == '' for e in temp):
                        empty_rows_counter += 1
                    else:
                        if empty_rows_counter == 0:
                            row = self.add_arr(row, temp)
                    return row, empty_rows_counter

                # for headers section
                if len(self.data_body) == 0:
                    if self.dual_headers:
                        self.headers += self.extract_array_data_from_big_spaces_pos(i)
                    else:
                        headers, empty_rows_counter = helper_x(self.headers, empty_rows_counter)

                # for body section
                else:
                    # handles body leftover row info
                    if self.dual_headers:
                        # i have to flatten this
                        self.data_body[-1] += self.extract_array_data_from_big_spaces_pos(i)

                    else:
                        new_temp_row = self.get_headers_overlapped_values(i)
                        self.data_body[-1] = self.add_arr(self.data_body[-1], new_temp_row)

        # filter out non rows
        self.data_body = list(filter(lambda x: x[0] != '', self.data_body))

        return

    def match_headers_char_position_overlap(self, line_inverse_pos):
        def check_overlap(a, b):
            foo = lambda x, y: True if x[-1] < y[-1] and x[-1] > y[0] else False
            if foo(a, b) or foo(b, a):
                return True
            return False

        overlapped_col_pos = []
        for j in line_inverse_pos:
            appended = False
            for i in range(len(self.headers_char_position)):
                if check_overlap(j, self.headers_char_position[i]):
                    overlapped_col_pos.append(i)
                    appended = True
                    break
            if not appended:
                overlapped_col_pos.append([])
        return overlapped_col_pos

    def get_headers_overlapped_values(self, line_position):
        line_data = self.text_line_data[line_position]
        big_spaces_pos = self.big_spaces_pos_list[line_position]

        headers_char_pos = self.headers_char_position
        temp_inverse_pos = self.get_inverse_pos(line_data, big_spaces_pos)
        col_pos = self.match_headers_char_position_overlap(temp_inverse_pos)
        row_d = self.extract_array_data_from_big_spaces_pos(line_position)
        
        result = []
        for i in range(len(headers_char_pos)):
            if col_pos.__contains__(i):
                result.append(row_d[col_pos.index(i)])
            else:
                result.append('')
        return result

    @staticmethod
    def check_if_single_item(row_d, big_spaces_pos):
        if len(big_spaces_pos) == 0:
            return True
        elif len(big_spaces_pos) == 1 and big_spaces_pos[0][0] == 0:
            return True
        elif len(big_spaces_pos) == 2 and big_spaces_pos[0][0] == 0 and big_spaces_pos[-1][1] == len(row_d) - 1:
            return True
        return False

    @staticmethod
    def add_arr(old, new):
        for i in range(len(old)):
            old[i] += ' ' + new[i] if new[i] != '' else ''
        return old

    @staticmethod
    def get_inverse_pos(array, subarr_pos):
        arr_len = len(array)
        tracker = 0
        result = []
        for i in subarr_pos:
            if i[0] != 0:
                result.append([tracker, i[0] - 1])
            tracker = i[1]
        result.append([tracker, arr_len])
        return result

    def get_dataframe(self):
        if not self.dataframe:
            data = {}
            for i in range(len(self.headers)):
                data[self.headers[i]] = {}
                temp_col = [x[i] for x in self.data_body]
                for j, v in enumerate(temp_col):
                    data[self.headers[i]][j] = v
            self.dataframe = pd.DataFrame(data)
        return self.dataframe


if __name__ == "__main__":
    data_path = os.getcwd().replace('python', 'data')
    items = os.listdir(data_path)
    files = list(filter(lambda x: os.path.isfile(data_path + '/' + x), items))

    dataframe_list = []

    for f in files:
        file_path = data_path + '/' + f
        temp_data = TableData(file_path, True) if f.__contains__('BOS') else TableData(file_path, False)
        dataframe_list.append(temp_data.get_dataframe())
