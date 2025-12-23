import argparse
import json
import pandas as pd

def read_ao(ao_file, ao_sheet="Technical Info"):
    df = pd.read_excel(ao_file, sheet_name=ao_sheet)

    df_source_info = df[['Technical Label', 'Technical Name']]
    df_source_info = df_source_info[df_source_info['Technical Label'].notna()]
    df_source_info = df_source_info[df_source_info['Technical Label'] == 'Query Technical Name']

    df_dimkf = df[['Characteristic/Key figure', 'Rows/Columns']]
    df_dimkf = df_dimkf[df_dimkf['Rows/Columns'].isin(["ROWS", "COLUMNS"])]
    df_dimkf = df_dimkf[['Characteristic/Key figure']]
    df_measures = df[['Displayed Measures']]
    df_dim_all = df[['Characteristic/Key figure']]
    return df_source_info, df_dimkf, df_measures, df_dim_all


def read_xtract(json_file):
    # --- 1. Read and Load JSON Data ---
    input_filename = json_file
    try:
        with open(input_filename, 'r') as file:
            # Load the file content into a Python dictionary
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"Error: The file {input_filename} was not found.")
        data = None
    return data


def write_json(json_file, data):
    with open(json_file, 'w') as file:
        # Dump the Python dictionary back into JSON format
        # 'indent=4' makes the output human-readable with 4-space indentation
        json.dump(data, file, indent=4)


def update_xtract_source_json():
    parser = argparse.ArgumentParser(description="AO -> XTRACT with logging")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    with open(args.config, "r") as fh:
        config = json.load(fh)
        fh.close()
    # Get the technical information from Analytic Excel File
    df_source_info, df_dim, df_measures, df_dim_all = read_ao(config.get("ao_file"), config.get("ao_file_tech_tab"))
    mode = config.get("mode")
    qry_name = df_source_info["Technical Name"][0]
    # Get the selected dimensions
    list_rows_cols = []
    for row in df_dim.itertuples():
        list_rows_cols.append(row[1])

    # Get the displayed measures
    df = df_measures['Displayed Measures'].notna()
    displayed_measures = ''
    list_displayed_measures = []
    for row in df_measures[df].itertuples():
        displayed_measures = row[1]
    list_displayed_measures = displayed_measures.split("; ")

    # read the definition of source.json
    df_xtract = read_xtract(config.get("source.json"))

    if mode == "MDX":
        if "mdxDefinition" in df_xtract and qry_name in df_xtract["mdxDefinition"]["name"]:
            # make isSelected = True for all displayed measures
            if "measures" in df_xtract["mdxDefinition"]:
                list_mdx_measures = df_xtract["mdxDefinition"]["measures"]
                list_selected_measures = [measure for measure in list_mdx_measures if
                                          measure["description"] in list_displayed_measures]
                for measure in list_selected_measures:
                    measure["isSelected"] = True
            # make isSelected = True for the selected dimensions
            if "dimensions" in df_xtract["mdxDefinition"]:
                list_mdx_dimensions = df_xtract["mdxDefinition"]["dimensions"]
                # ignore the navigational attributes
                list_selected_dimensions = [dimension for dimension in list_mdx_dimensions if
                                            "__" not in dimension["shortName"]
                                            and dimension["dimensionType"] not in ["NormalWithNoAggregation",
                                                                                   "Measure"]]  # in list_rows_cols
                list_selected_attributes = [dimension for dimension in list_mdx_dimensions if
                                            "__" in dimension["shortName"]
                                            and dimension["dimensionType"] not in ["NormalWithNoAggregation", "Measure"]]
                list_dim_shortname = [dimension["shortName"] for dimension in list_mdx_dimensions]
                for l in list_selected_attributes:
                    d = l["shortName"].split("__")
                    # if attribute exists, but no dimension, we have to select the attributes
                    if d[0] not in list_dim_shortname:
                        list_selected_dimensions.append(l)
                for dimension in list_selected_dimensions:
                    print(dimension["shortName"])
                    #if dimension["shortName"] == "0BUS_AREA":
                    dimension["isSelected"] = True

                    for prop in dimension["properties"]:
                        if prop["caption"] == "Key":
                            prop["isSelected"] = True

            # Output the updated dict to a json file
            write_json("output_mdx_source.json", df_xtract)
    else:
        if "bicsDefinition" in df_xtract:
            # make isSelected = True for all measures
            if "measures" in df_xtract["bicsDefinition"]:
                list_bics_measures = df_xtract["bicsDefinition"]["measures"]
                for measure in list_bics_measures:
                    measure["isSelected"] = True
            # make isSelected = True for the all dimensions
            if "dimensions" in df_xtract["bicsDefinition"]:
                list_bics_dimensions = df_xtract["bicsDefinition"]["dimensions"]
                for d in list_bics_dimensions:
                    if d["containsKeyFigures"] != True:  # key figure structure
                        d["isSelected"] = True

            # Output the updated dict to a json file
            # write_json("output_bics_source.json", df_xtract)
            write_json(config.get("source.json"), df_xtract)
            # Permission Denied
            # write_json(config.get("source.json"), df_xtract)


if __name__ == "__main__":
    update_xtract_source_json()
