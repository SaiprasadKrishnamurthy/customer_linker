"""
    Parses 26AS and extracts the Legal Name and TAN and dumps it to a corresponding output file.
"""
import os


class LegalNameToTan:
    def __init__(self, legal_name, tan):
        self.legal_name = legal_name
        self.tan = tan


def parse_26as(folder, twenty_six_as_out_file):
    legal_names_to_tans = []
    absolute = os.path.abspath(folder)
    for x in os.listdir(folder):
        if x.endswith(".txt"):
            filename = x
            with open(absolute + os.sep + filename) as fp:
                line = fp.readline()
                while line:
                    parts = line.split("^")

                    if len(parts) > 3 and len(parts[2]) == 10 and parts[2].isalnum():
                        obj = LegalNameToTan(parts[1].strip(), parts[2].strip())
                        if obj not in legal_names_to_tans:
                            legal_names_to_tans.append(obj)
                    line = fp.readline()
    with open(twenty_six_as_out_file, 'w') as f:
        f.write('26as_name\ttan\n')
        for obj in legal_names_to_tans:
            f.write(f'{obj.legal_name}\t{obj.tan}\n')

    print(f"Extracted all Legal Names and TANs From 26AS Files to {twenty_six_as_out_file}")
