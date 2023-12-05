# Import necessary libraries
from pinotdb import connect  # PinotDB for connecting to Pinot database
import streamlit as st  # Streamlit for creating web applications
import pandas as pd  # Pandas for data manipulation and analysis
import plotly.express as px  # Plotly Express for interactive visualizations
import time  # Time for time-related operations
import datetime  # Datetime for working with dates and times
import altair as alt  # Altair for declarative statistical visualization

# Define a list of column names
COLUMNS_LIST = ["Dir", "Dir_one", "Dir_others", "Dport", "DstAddr", "Dur", "Proto", "Proto_others", "Proto_tcp", "Proto_udp",
                "Sport", "SrcAddr", "SrcBytes", "StartTime", "State", "TotBytes", "TotPkts", "dTos", "prediction", "sTos",
                "sTosone"]

# Set up Streamlit page configuration
st.set_page_config(
    page_title="Real-Time NIDS Dashboard",
    page_icon="🔒",
    layout="wide",
)
# Display the title and last update time
st.markdown("""
# Network Intrusion Detection System Dashboard
""")
now = datetime.datetime.now()
dt_string = now.strftime("%d %B %Y %H:%M:%S")
st.write(f"Last update: {dt_string}")

# Set up Streamlit session state variables for auto-refresh
if not "sleep_time" in st.session_state:
    st.session_state.sleep_time = 2

if not "auto_refresh" in st.session_state:
    st.session_state.auto_refresh = True

# Enable auto-refresh checkbox and set up refresh rate
auto_refresh = st.checkbox('Auto Refresh?', st.session_state.auto_refresh)
col61, col62 = st.columns(2)

# Update refresh rate if auto-refresh is enabled
if auto_refresh:
    number = col61.number_input('Refresh rate in seconds', value=st.session_state.sleep_time)
    st.session_state.sleep_time = number

# Connect to Pinot database
conn = connect(host='localhost', port=7001, path='/query/sql', scheme='http')

# Function to access Pinot database and retrieve data
def db_access(query):
    curs = conn.cursor()
    curs.execute(f"SELECT * FROM logslabelled ORDER BY StartTime DESC LIMIT {query}")
    df = pd.DataFrame(curs, columns=COLUMNS_LIST)
    return df

# Function to access Pinot database with time range and retrieve data
def db_accesstwo(starttime, endtime, limit):
    curs = conn.cursor()
    curs.execute(f"SELECT * FROM logslabelled WHERE StartTime BETWEEN '{starttime}' AND '{endtime}' ORDER BY StartTime DESC LIMIT {limit};")
    df = pd.DataFrame(curs, columns=COLUMNS_LIST)
    return df

# Function to query Pinot database with specific columns and category
def query_pinot(start_time, end_time, selected_columns, selected_category):
    selected_columns = ['StartTime', 'SrcAddr', 'DstAddr'] + selected_columns + ['prediction']
    columns_to_select = "*" if not selected_columns else ", ".join(selected_columns)
    
    pinot_query = f"""
    SELECT
      {columns_to_select}
    FROM
      logslabelled
    WHERE
      StartTime >= '{start_time}' AND StartTime <= '{end_time}'
      AND (prediction = {selected_category}) LIMIT 100
    """
    conn = connect(host='localhost', port=7001, path='/query/sql', scheme='http')
    curs = conn.cursor()
    curs.execute(pinot_query)
    df = None
    if len(selected_columns) > 0:
        df = pd.DataFrame(curs, columns=selected_columns)
    else:
        df = pd.DataFrame(curs, columns=COLUMNS_LIST)
    return df

# Set up the Streamlit UI
option = col62.selectbox(
    'Select the number of logs?',
    ('100', '1000', '5000'))

# Function to display prediction counts
def countshow(color="green"):
    df = db_access(option)
    st.title('Prediction Counts')
    prediction_counts = df['prediction'].value_counts()
    col11, col12, col13 = st.columns(3)
    font_size = 20
    li = ["Background", "Botnet", "Normal"]
    for prediction_value, count in prediction_counts.items():
        with col11 if prediction_value == 0 else col12 if prediction_value == 1 else col13:
            temp = int(prediction_value)
            st.metric(label=f'{li[temp]}', value=count)

def pieCharts():
    df = db_access(int(option))
    required_columns = ['SrcAddr', 'DstAddr', 'Dport', 'prediction']
    col1, col2, col3, col4 = st.columns(4)
    chart_width = 400
    chart_height = 400
    font_size = 12
    title_color = "Green"

    for index, column in enumerate(required_columns):
        with col1 if index % 4 == 0 else col2 if index % 4 == 1 else col3 if index % 4 == 2 else col4:
            # Calculate the top 10 legend entries
            top_legend_entries = df[column].value_counts().nlargest(10).index.tolist()

            # Filter the DataFrame to include only the top 10 legend entries
            filtered_df = df[df[column].isin(top_legend_entries)]

            # Create a pie chart with the filtered DataFrame
            fig = px.pie(filtered_df, names=column, title=column)

            # Customize the pie chart layout
            fig.update_traces(showlegend=True)
            fig.update_layout(
                width=chart_width,
                height=chart_height,
                font=dict(size=font_size),
                title_text="<b>" + column + "</b>",
                title_x=0.3,
                title_y=0.98,
                title_font=dict(size=30, color=title_color)
            )

            # Display the modified pie chart
            st.plotly_chart(fig)


# Function to display protocol vs. time histogram
def protocolTime():
    df = db_access(option)
    x_column = "StartTime"
    y_column = "Proto"

    st.markdown(f"## Protocol vs. Time")

    fig = px.histogram(df, x=x_column, color=y_column, nbins=100)

    fig.update_layout(
        xaxis_title="StartTime",
        yaxis_title="Count",
    )
    st.plotly_chart(fig)

# Function to display notifications
def show_notification(message, color='info'):
    st.toast(message, icon='🔔')

def notify(message, icon="🔔", color="red"):
    st.markdown(
        f"""<div style="position: fixed; top: 100px; right: 600px; padding: 10px; background-color: {color}; color: white; border-radius: 5px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);">{icon} {message}</div>""",
        unsafe_allow_html=True,
    )

# Function to display scatter plot for durations over time
def duraScatter():
    df = db_access(option)
    st.markdown("<h1 style='text-align: left; color: white; font-size: 30px;'>Scatter Plot for Durations Over Time</h1>", unsafe_allow_html=True)
    scatter_fig = px.scatter(df, x="StartTime", y="Dur", title="Durations Over Time")
    st.plotly_chart(scatter_fig)

# Function to display correlation heatmap
def correl():
    st.markdown("<h1 style='text-align: left; color: white; font-size: 30px;'>Correlation Heatmap</h1>", unsafe_allow_html=True)
    df = db_access(option)
    sel_cols = ["SrcBytes", "TotBytes", "TotPkts"]
    df = df[sel_cols]
    correlation_matrix = df.corr()
    st.write(correlation_matrix)

# Function to display histogram of durations
def durhist():
    df = db_access(option)
    st.subheader("Distribution of Durations")
    fig = px.histogram(df, x="Dur", nbins=100, marginal="rug")
    st.plotly_chart(fig)

# Function to display dashboard metrics
def commmetrics():
    df = db_access(option)
    total_records = len(df)
    unique_ip_addresses = df['SrcAddr'].nunique()
    attacks_detected = df[df['prediction'] == 1.0]['prediction'].count()
    st.header('Dashboard Metrics')
    st.markdown(f'Total Records: {total_records}')
    st.markdown(f'Unique IP Addresses: {unique_ip_addresses}')
    st.markdown(f'Attacks Detected: {attacks_detected}')

# Function to display pair plots
def pairplots():
    df = db_access(option)
    numeric_columns = st.multiselect("Select Numeric Columns", df.select_dtypes(include='number').columns)
    if len(numeric_columns) < 2:
        st.warning("Please select at least two numeric columns.")
    else:
        chart = alt.Chart(df).mark_circle(size=100).encode(
            alt.Color('prediction:N', scale=alt.Scale(scheme='accent')),
            alt.Tooltip(numeric_columns),
            alt.X(alt.repeat("column"), type='quantitative'),
            alt.Y(alt.repeat("row"), type='quantitative'),
        ).properties(
            width=250,
            height=250,
        ).repeat(
            column=numeric_columns,
            row=numeric_columns
        ).interactive()
        st.altair_chart(chart, use_container_width=True)

# Function to display histogram of total packets
def totpkthist():
    df = db_access(option)
    numeric_cols = ["TotPkts"]
    st.markdown("<h1 style='text-align: left; color: white; font-size: 30px;'>TotPkts Histogram</h1>", unsafe_allow_html=True)
    df_numeric = df[numeric_cols]
    num_bins = st.slider("Select number of bins", min_value=20, max_value=80, value=20)
    chart = alt.Chart(df_numeric).mark_bar(color="aqua").encode(
        alt.X("TotPkts:Q", bin=alt.Bin(maxbins=num_bins)),
        y='count()',
    ).properties(
        width=600,
        height=400
    )

    st.altair_chart(chart, use_container_width=True)

# Function to display queries and results
def doit():
    st.title("Log Data")
    col41, col42, col43, col44 = st.columns(4)
    cols_list = COLUMNS_LIST
    temp = ['StartTime', 'prediction', 'SrcAddr', 'DstAddr']
    for i in temp:
        cols_list.remove(i)
    with col41:
        start_date = st.date_input("Select start date", pd.to_datetime("2011-08-01"))
        end_date = st.date_input("Select end date", pd.to_datetime("2011-08-01"))
    with col42:
        selected_columns = st.multiselect('Select Columns to Display', cols_list)
    with col43:
        selected_category = st.selectbox('Select Prediction Category', ['0', '1', '2'])
    def apply_row_formatting(row):
        if row["prediction"] == 0.0:
            return ['background-color: yellow'] * len(row)
        elif row["prediction"] == 1.0:
            return ['background-color: red'] * len(row)
        elif row["prediction"] == 2.0:
            return ['background-color: green'] * len(row)
        else:
            return [''] * len(row)
    df = query_pinot(start_date, end_date, selected_columns, selected_category)
    if selected_category == '0':
        col44.metric("Background", df[df["prediction"] == 0.0].shape[0])
    elif selected_category == '1':
        col44.metric("Botnet", df[df["prediction"] == 1.0].shape[0])
    else:
        col44.metric("Normal Traffic", df[df["prediction"] == 2.0].shape[0])
    df = df.iloc[:100]
    formatted_df = df.style.apply(apply_row_formatting, axis=1)
    st.dataframe(formatted_df, hide_index=True, use_container_width=True)

# Function to display network activity over time
def netactiv():
    df = db_access(option)
    fig = px.line(df.iloc[:100], x='StartTime', y=['TotPkts', 'TotBytes'], title='Network Activity Over Time')
    fig.update_layout(xaxis_title='Time', yaxis_title='Count', legend_title='Metric')
    st.plotly_chart(fig)

# Function to display line charts for TotPkts and TotBytes
def niceones():
    st.title("Line charts for TotPkts and TotBytes")
    df = db_access(option)
    df = df.iloc[:300]
    st.line_chart(df.set_index('StartTime')['TotPkts'])
    st.line_chart(df.set_index('StartTime')['TotBytes'])

# Display various sections of the dashboard
col71, col72, col73 = st.columns(3)
with col71:
    countshow()
with col72:
    commmetrics()
with col73:
    correl()
pieCharts()
col21, col22 = st.columns(2)
with col22:
    netactiv()
with col21:
    durhist()
col31, col32 = st.columns(2)
with col31:
    protocolTime()
with col32:
    totpkthist()
niceones()
doit()

# Auto-refresh the page if enabled
if auto_refresh:
    time.sleep(number)
    st.rerun()
