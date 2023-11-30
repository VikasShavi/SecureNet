from pinotdb import connect
import streamlit as st
import pandas as pd
import plotly.express as px
import time
import datetime
import altair as alt

COLUMNS_LIST = ["Dir", "Dir_one", "Dir_others", "Dport", "DstAddr", "Dur", "Proto", "Proto_others", "Proto_tcp", "Proto_udp",
                "Sport", "SrcAddr", "SrcBytes", "StartTime", "State", "TotBytes", "TotPkts", "dTos", "prediction", "sTos",
                "sTosone"]

st.set_page_config(
    page_title="Real-Time NIDS Dashboard",
    page_icon="✅",
    layout="wide",
)
st.markdown("""
# Network Intrusion Detection System Dashboard
""")
conn = connect(host='localhost', port=7001, path='/query/sql', scheme='http')
def db_access(query):
    # Replace with your Pinot broker information
    curs = conn.cursor()
    curs.execute(f"SELECT * FROM logslabelled ORDER BY StartTime DESC LIMIT {query}")
    df = pd.DataFrame(curs, columns=COLUMNS_LIST)
    return df


option = st.selectbox(
        'Select the number of logs?',
        ('100', '1000', '5000'))


def countshow(color="green"):
    df = db_access(option)
    st.title('Prediction Counts')
    prediction_counts = df['prediction'].value_counts()
    col11, col12, col13 = st.columns(3)
    # Display each count as a metric
    font_size = 20
    li = ["Background", "Botnet", "Normal"]
    for prediction_value, count in prediction_counts.items():
        with col11 if prediction_value == 0 else col12 if prediction_value == 1 else col13:
            temp = int(prediction_value)
            st.metric(label=f'{li[temp]}', value=count)


def pieCharts():
    df = db_access(int(option) / 25)
    # List of required columns
    required_columns = ['SrcAddr', 'DstAddr', 'Dport']
    # Create a Streamlit layout with three columns for the pie charts
    col1, col2, col3 = st.columns(3)
    # Define the width and height for the pie charts
    chart_width = 450  # Adjust the width as needed
    chart_height = 450  # Adjust the height as needed
    # Define the font size for the pie charts
    font_size = 8  # Adjust the font size as needed
    # Define title color
    title_color = "Green"  # Adjust the color as needed
    # Create pie charts for the required columns using Plotly
    for index, column in enumerate(required_columns):
        with col1 if index % 3 == 0 else col2 if index % 3 == 1 else col3:
            # st.write(f"**{column}**")
            fig = px.pie(df, names=column, title=column)
            fig.update_traces(showlegend=True)  # Show the legend
            fig.update_layout(width=chart_width, height=chart_height,
                            font=dict(size=font_size),
                            title_text="<b>" + column + "</b>",  # Bold title
                                title_x=0.3,  # Center the title
                                title_y=0.98,  # Adjust the title position
                                title_font=dict(size=30, color=title_color)) # Title font size and color)
            st.plotly_chart(fig)

def protocolTime():
    df = db_access(option)
    # Define the column for the X-axis (Time) and Y-axis (Protocol)
    x_column = "StartTime"
    y_column = "Proto"

    # Create a Streamlit layout for the histogram
    st.markdown(f"# Protocol vs. Time")

    # Create the histogram using Plotly Express
    fig = px.histogram(df, x=x_column, color=y_column, nbins=100)

    # Customize the appearance of the histogram
    fig.update_layout(
        xaxis_title="StartTime",
        yaxis_title="Count",
        # title="Protocol vs. Time Histogram",
    )
    # Show the histogram in Streamlit
    st.plotly_chart(fig)

def show_notification(message, color='info'):
    st.toast(message, icon='🔔')

def notify(message, icon="🔔", color="red"):
    st.markdown(
        f"""<div style="position: fixed; top: 100px; right: 600px; padding: 10px; background-color: {color}; color: white; border-radius: 5px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);">{icon} {message}</div>""",
        unsafe_allow_html=True,
    )

def duraScatter():
    df = db_access(option)
    # Streamlit App
    st.title("Scatter Plot for Durations Over Time")
    # Scatter Plot
    scatter_fig = px.scatter(df, x="StartTime", y="Dur", title="Durations Over Time")

    st.plotly_chart(scatter_fig)

def correl():
    st.title("Correlation Heatmap")
    df = db_access(option)
    sel_cols = ["SrcBytes", "TotBytes", "TotPkts"]
    df = df[sel_cols]
    # Calculate correlation matrix
    correlation_matrix = df.corr()
    st.write(correlation_matrix)
    # Display heatmap using Seaborn

def durhist():
    df = db_access(option)
    # Histogram for Duration using Plotly
    st.subheader("Distribution of Durations")
    fig = px.histogram(df, x="Dur", nbins=100, marginal="rug")
    st.plotly_chart(fig)

def commmetrics():
    df = db_access(option)
    # Dashboard Metrics
    total_records = len(df)
    unique_ip_addresses = df['SrcAddr'].nunique()
    attacks_detected = df[df['prediction'] == 1.0]['prediction'].count()

    # Streamlit App
    st.title('Network Intrusion Detection Dashboard')

    # Display Metrics
    st.header('Dashboard Metrics')
    st.markdown(f'Total Records: {total_records}')
    st.markdown(f'Unique IP Addresses: {unique_ip_addresses}')
    st.markdown(f'Attacks Detected: {attacks_detected}')

    # Visualizations (Add your visualizations here)

    # Data Table
    st.header('Sample Data Table')
    st.dataframe(df.head())

def pairplots():
    df = db_access(option)
    numeric_columns = st.multiselect("Select Numeric Columns", df.select_dtypes(include='number').columns)
    # Check if at least two numeric columns are selected
    if len(numeric_columns) < 2:
        st.warning("Please select at least two numeric columns.")
    else:
        # Define custom colors for each prediction class
        color_scale = alt.Scale(domain=['Background', 'Botnet', 'Normal'],
                                range=['red', 'red', 'red'])
        # Create a pair plot using Altair
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

        # Display the pair plot using Streamlit
        st.altair_chart(chart, use_container_width=True)


def totpkthist():
    df = db_access(option)
    # Extract numeric columns from the DataFrame
    numeric_cols = ["TotPkts"]
    # Create a DataFrame with only numeric columns
    df_numeric = df[numeric_cols]
    # Choose numeric columns for the pair plot
    # Increase the number of bins
    num_bins = st.slider("Select number of bins", min_value=1, max_value=100, value=20)
    # Box Plot using Altair
    # Plot histogram using Altair with increased bins
    chart = alt.Chart(df_numeric).mark_bar(color="aqua").encode(
        alt.X("TotPkts:Q", bin=alt.Bin(maxbins=num_bins)),
        y='count()',
    ).properties(
        width=600,
        height=400
    )

    # Display the chart using Streamlit
    st.altair_chart(chart, use_container_width=True)


st.sidebar.title("Network Intrusion Detection")
now = datetime.datetime.now()
dt_string = now.strftime("%d %B %Y %H:%M:%S")
st.sidebar.write(f"Last update: {dt_string}")

if not "sleep_time" in st.session_state:
    st.session_state.sleep_time = 5

if not "auto_refresh" in st.session_state:
    st.session_state.auto_refresh = True

auto_refresh = st.sidebar.checkbox('Auto Refresh?', st.session_state.auto_refresh)

if auto_refresh:
    number = st.sidebar.number_input('Refresh rate in seconds', value=st.session_state.sleep_time)
    st.session_state.sleep_time = number

# Example usage
if option == '1000':
    show_notification("Hello baby how do you do?")
if st.button('Three cheers'):
    st.toast('Hip!')
    time.sleep(.5)
    st.toast('Hip!')
    time.sleep(.5)
    st.toast('Hooray!', icon='🎉')

countshow()
pieCharts()
protocolTime()
col21, col22, col23 = st.columns(3)
with col21:
    duraScatter()
with col23:
    correl()
totpkthist()
if auto_refresh:
    time.sleep(number)
    st.rerun()