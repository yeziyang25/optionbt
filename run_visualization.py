import os
cur_dir = os.path.dirname(__file__)
import sys
sys.path.append('Z:\\ApolloGX')
import im_prod.std_lib.visualization as visualization

if __name__ == "__main__":
    output_folder = "C:\\Users\\bkwok\\dev\\backtest2\\output\\BTCC_backtest\\"
    visualization.create_ppt_chart_pkg(output_folder + "visualization_config_template.csv", output_folder + "visualized_output.ppt")