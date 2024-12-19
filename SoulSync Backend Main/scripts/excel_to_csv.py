import pandas as pd
import sys

excel_filename = sys.argv[1]

if (excel_filename.endswith('.xlsx')):
  csv_filename = excel_filename.removesuffix('.xlsx') + '.csv'
elif (excel_filename.endswith('.xls')):
  csv_filename = excel_filename.removesuffix('.xls') + '.csv'
else:
  raise Exception("file must be a valid excel file")

excel_file = pd.read_excel(excel_filename)
excel_file.to_csv(csv_filename, header=True, index=False)