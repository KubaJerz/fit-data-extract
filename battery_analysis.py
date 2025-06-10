import sys
import pandas as pd
from pathlib import Path
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import numpy as np

def analyze_battery_life(folder_path):
    """
    Reads all .csv files in the specified folder, concatenates them,
    performs linear regression to predict battery life, and calculates RMSE.

    Args:
        folder_path (Path): The path to the folder containing .csv files.
    """
    all_data = []

    # Iterate over all .csv files in the folder
    for file_path in folder_path.glob('*.csv'):
        try:
            df = pd.read_csv(file_path)
            all_data.append(df)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue

    if not all_data:
        print(f"No .csv files found in {folder_path} or unable to read them.")
        return

    # Concatenate all dataframes
    combined_df = pd.concat(all_data, ignore_index=True)

    # --- Start of the corrected section ---

    # 2. Convert 'timestamp' to datetime objects.
    #    CRITICAL: Verify the 'format' string matches your actual data format.
    #    For example, if your year is four digits (e.g., 2024), use %Y instead of %y.
    #    If your date is DD/MM/YY, use %d/%m/%y.

    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], format='mixed')



    first_time_stamp = combined_df['timestamp'].min()
    combined_df['time_elapsed_hours'] = (combined_df['timestamp'] - first_time_stamp).dt.total_seconds() / 3600

    X = combined_df[['time_elapsed_hours']].values



    y = combined_df['developer_field'].values  # Dependent variable (battery percentage)

    # Add a check for sufficient data points for regression
    if len(X) < 2 or len(y) < 2:
        print("Not enough data points (at least 2 required) to perform linear regression.")
        return

    # Initialize and train the Linear Regression model
    model = LinearRegression()
    model.fit(X, y)

    # Predict battery percentage based on the model
    y_pred = model.predict(X)

    # Calculate RMSE
    rmse = np.sqrt(mean_squared_error(y, y_pred))

    # Predict how many hours the battery will last (when battery_percentage hits 0)
    # The linear equation is y = mx + c, where y is battery_percentage, x is time_elapsed_hours
    # We want to find x when y = 0
    # 0 = mx + c  =>  mx = -c  =>  x = -c / m

    # Ensure the slope (m) is not zero to avoid division by zero
    if model.coef_[0] == 0:
        print("Error: The battery percentage does not seem to change with time (slope is zero). Cannot reliably predict battery life.")
        predicted_hours = np.nan
    else:
        # The predicted_hours value will be the time_elapsed_hours at which 'developer_field' is 0
        predicted_hours = -model.intercept_ / model.coef_[0]

    print(f"--- Battery Analysis Results for '{folder_path.name}' ---")
    print(f"Predicted total battery life (from start of measurements): {predicted_hours:.2f} hours")
    print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")

def main():
    """Main entry point for the script."""
    if len(sys.argv) < 2:
        print("Usage: python battery_analysis.py /path/to/fit/folder")
        sys.exit(1)

    input_path = Path(sys.argv[1])

    if not input_path.exists():
        print(f"Error: Path '{input_path}' does not exist")
        sys.exit(1)

    if not input_path.is_dir():
        print(f"Error: Path '{input_path}' is not a directory")
        sys.exit(1)

    analyze_battery_life(input_path)

if __name__ == "__main__":
    main()