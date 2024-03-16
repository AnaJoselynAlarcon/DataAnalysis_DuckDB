# %%
import pandas as pd
import glob
import time
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns


# %%
conn = duckdb.connect()  # create an in-memory database
# %%

df = conn.execute("""
	SELECT *
	FROM read_csv_auto('dataset/*.csv', header=TRUE, filename=True)
	
""").df()

print(df)

# %%
conn.register("df_view", df)
conn.execute("DESCRIBE df_view").df()

# %%
conn.execute("SELECT COUNT(*) FROM df_view").df()

# %%
# Check for null values
df.isnull().sum()


# %%
conn.execute("""
    CREATE OR REPLACE TEMPORARY TABLE temp_table AS
    SELECT *
    FROM df_view
    WHERE "06.Detection Date" <> 'CONFIDENTIAL'
""")

conn.execute("SELECT COUNT(*) FROM temp_table").df()

# %%
# Recreate the view based on the temporary table
conn.execute("CREATE OR REPLACE VIEW df_view AS SELECT * FROM temp_table")

# %%
conn.execute("""
        CREATE OR REPLACE TABLE failure_report AS
             SELECT
                "01.Licence Number" AS licence_number,
                "02.Current Licence Status" AS licence_status,
                "03.Licensee Name" AS licensee_name,
                "04.Orig BA Code" AS orig_ba_code,
                "05.Surface Location" AS surface_location,
                strptime("06.Detection Date", '%b %d, %Y')::DATE AS detection_date,
                strptime("07.Report Date", '%b %d, %Y')::DATE AS report_date,
                "08.Report Status" AS report_status,
                "09.Failure Type" AS failure_type,
                "10.Failure Top Depth (mKB)"::INTEGER AS failure_top_depth,
                "11.Failure Depth Bottom (mKB)"::INTEGER AS failure_bottom_depth,
                "12.Reported Resolution" AS reported_resolution,
                strptime("13.Reported Resolution Date", '%b %d, %Y')::DATE AS reported_resolution_date,
                "14.Steam Scheme Type" AS steam_scheme_type,
                "15.Connection Type" AS connection_type,
                strptime("16.Final Drill Date", '%b %d, %Y')::DATE AS final_drill_date,
             FROM temp_table
             """)
# %%
conn.execute("FROM failure_report").df()


# %%
failureTypeCount = conn.execute(
    "SELECT failure_type, COUNT(*) AS count FROM failure_report GROUP BY failure_type ORDER BY count DESC").df()

# %%
# Plot the data
plt.figure(figsize=(10, 6))
sns.barplot(x='failure_type', y='count', data=failureTypeCount)
plt.xlabel('Failure Type')
plt.ylabel('Count')
plt.title('Distribution of Failure Types')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
# %%
# starting dates visualization
date_data = conn.execute(
    "SELECT detection_date, report_date, reported_resolution_date, final_drill_date FROM failure_report").df()
# %%
# Convert the data types to datetime
date_data['detection_date'] = pd.to_datetime(date_data['detection_date'])
date_data['report_date'] = pd.to_datetime(date_data['report_date'])
date_data['reported_resolution_date'] = pd.to_datetime(
    date_data['reported_resolution_date'])
date_data['final_drill_date'] = pd.to_datetime(date_data['final_drill_date'])
# %%
date_data = date_data.melt(var_name='Date Type', value_name='Date')

# Plot the data
# plt.figure(figsize=(10, 6))
# sns.histplot(data=date_data, x='Date', hue='Date Type', kde=False, bins=50)
# plt.xlabel('Date')
# plt.ylabel('Frequency')
# plt.title('Distribution of Dates')
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.show()
# %%

plt.figure(figsize=(10, 6))
sns.lineplot(data=date_data, x='Date', y='Date', hue='Date Type',
             style='Date Type', markers=True, dashes=False)
plt.xlabel('Date')
plt.ylabel('Count')
plt.title('Trends of Dates Over Time')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %%
