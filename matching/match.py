import pandas as pd
from name_matching.name_matcher import NameMatcher


def match(sales_output_file, twenty_six_as_output_file, output_file):
    a = pd.read_csv(sales_output_file, sep='\t')
    b = pd.read_csv(twenty_six_as_output_file, sep='\t')

    matcher = NameMatcher(ngrams=(2, 5),
                          top_n=10,
                          number_of_rows=500,
                          number_of_matches=3,
                          lowercase=True,
                          punctuations=True,
                          remove_ascii=True,
                          legal_suffixes=True,
                          common_words=False,
                          preprocess_split=True,
                          verbose=True)

    matcher.set_distance_metrics(['iterative_sub_string', 'pearson_ii', 'bag', 'fuzzy_wuzzy_partial_string', 'editex'])
    matcher.load_and_process_master_data('26as_name', b, transform=True)

    matches = matcher.match_names(to_be_matched=a, column_matching='sales_name')

    complete_matched_data = pd.merge(pd.merge(b, matches, how='left', right_index=True, left_index=True), a, how='left',
                                     left_on='match_index_0', right_index=True, suffixes=['', '_matched'])

    m = matches.loc[:, ['original_name', 'match_name_0', 'score_0', 'match_index_0']]

    matched_df = pd.DataFrame(columns=["sales_name", "gstin", "26as_name", "tan"])

    for i in range(0, len(m)):
        b_row = b.iloc[int(m.iloc[i]['match_index_0'])]
        b_company_name = b_row['26as_name']
        b_identifier = b_row['tan']
        if m.iloc[i]['score_0'] > 50.0:
            dict = {'sales_name': [m.iloc[i]['original_name']], 'gstin': [a.iloc[i]['gstin']],
                    '26as_name': [b_company_name], 'tan': [b_identifier], 'pan': [a.iloc[i]['gstin'][2:12]]}
            df = pd.DataFrame(dict)
            matched_df = pd.concat([matched_df, df], ignore_index=True)
    matched_df.to_csv(output_file, sep='\t')
    print('Results exported to txt file')
