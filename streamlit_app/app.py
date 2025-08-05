import os
import pandas as pd
import streamlit as st
import altair
import subprocess
import datetime
import plotly.graph_objects as go

# Absolute path for the timestamp file and setup.sh script
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TIMESTAMP_FILE = os.path.join(PROJECT_ROOT, 'last_run_date.txt')
SETUP_SCRIPT = os.path.join(PROJECT_ROOT, 'setup.sh')
DATA_FILEPATH = os.path.join(PROJECT_ROOT, 'data', 'stock_data_delta.parquet')

def run_setup_script():
    """Runs the setup.sh script."""
    try:
        subprocess.run(['bash', SETUP_SCRIPT], check=True)
        st.write("Setup script executed successfully.")
    except subprocess.CalledProcessError as e:
        st.error(f"Error executing setup script: {e}")

def check_and_run_setup():
    """Check if the setup script needs to run (once a day)."""
    if os.path.exists(TIMESTAMP_FILE):
        with open(TIMESTAMP_FILE, 'r') as f:
            last_run_date = f.read().strip()
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        if last_run_date != current_date:
            # The script hasn't been run today, so run it and update the timestamp
            run_setup_script()
            with open(TIMESTAMP_FILE, 'w') as f:
                f.write(current_date)
    else:
        # No timestamp file exists, so run the script for the first time
        run_setup_script()
        with open(TIMESTAMP_FILE, 'w') as f:
            f.write(datetime.datetime.now().strftime('%Y-%m-%d'))

# Check if we need to run the setup script
check_and_run_setup()



@st.cache_data
def load_data():
    return pd.read_parquet(DATA_FILEPATH)

df = load_data()

last_close_date = df['Date'].max()


# set page layout
st.set_page_config(layout='wide', page_title='Stock Market Analysis')

st.title("📈 Stock Market Dashboard")
st.markdown(f'Explore stock metrics for 30 popular tickers using three years of historic data')
st.markdown("Use the sidebar to explore a ticker. You can compare KPIs and identify trends across different metrics.")

# set sidebar
tickers = df['Ticker'].unique().tolist()
selected_ticker = st.sidebar.selectbox('Select a ticker', tickers)

filtered_df = df[df['Ticker'] == selected_ticker]

# show last closing price date
st.metric('Last Refresh Date', last_close_date.strftime('%Y-%m-%d'))

# show summary metrics 
st.subheader(f'Summary metrics for {selected_ticker}')
latest = filtered_df.sort_values('Date').iloc[-1]

col1, col2, col3 = st.columns(3)
col1.metric('Latest Close', f"${latest['Close']:.2f}")
col2.metric('Cumulative Return', f"{latest['Cumulative_Return']:.2f}%")
col3.metric('7-Day Momentum', f"{latest['Momentum_7d']}%")

st.subheader("📊 Visual Metrics")

tab1, tab2, tab3, tab4 = st.tabs(["📉 Price & Averages", "📈 Returns & Momentum", "📊 Volatility", "🧮 Technical Indicators"])

with tab1:
    st.markdown("**Candlestick Chart with Moving Averages**")
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=filtered_df['Date'],
        open=filtered_df['Open'],
        high=filtered_df['High'],
        low=filtered_df['Low'],
        close=filtered_df['Close'],
        name='Candlestick'
    ))

    fig.add_trace(go.Scatter(
        x=filtered_df['Date'],
        y=filtered_df['MA_50'],
        mode='lines',
        line=dict(color='green', width=1),
        name='MA 50'
    ))

    fig.add_trace(go.Scatter(
        x=filtered_df['Date'],
        y=filtered_df['MA_200'],
        mode='lines',
        line=dict(color='orange', width=1),
        name='MA 200'
    ))

    fig.update_layout(
        xaxis_title='Date',
        yaxis_title='Price',
        xaxis_rangeslider_visible=False,
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.markdown("**Daily Returns and Momentum**")
    line1 = altair.Chart(filtered_df).mark_line(color='steelblue').encode(
        x="Date:T",
        y=altair.Y("Daily_return:Q", title="Daily Return")
    )
    line2 = altair.Chart(filtered_df).mark_line(color='firebrick').encode(
        x="Date:T",
        y="Momentum_7d:Q"
    )
    st.altair_chart(line1 + line2, use_container_width=True)

with tab3:
    st.markdown("**Volatility Over Time**")
    vol_chart = altair.Chart(filtered_df).mark_area(opacity=0.4, color='gray').encode(
        x="Date:T",
        y="Volatility:Q"
    )
    st.altair_chart(vol_chart, use_container_width=True)

with tab4:
    macd_chart = altair.Chart(filtered_df).transform_fold(
        ['MACD', 'Signal_line'],
        as_=['Indicator', 'Value']
    ).mark_line().encode(
        x='Date:T',
        y='Value:Q',
        color='Indicator:N'
    ).properties(title='MACD vs Signal_line')

    st.altair_chart(macd_chart)

    draw_down_chart = altair.Chart(filtered_df).mark_area(opacity=0.5,color='crimson').encode(
        x='Date:T',
        y=altair.Y('DrawDown:Q', title='DrawDown (%)')
    ).properties(title='Draw Down Over Time')

    st.altair_chart(draw_down_chart)

# === KPI Comparison Section ===
st.subheader("📌 Compare KPIs Across Tickers")

kpi_options = ['Close', 'Volume', 'Daily_return', 'Volatility', 'Momentum_7d', 'Cumulative_Return']
selected_kpi = st.selectbox("Select KPI to Compare", kpi_options)

tickers_to_compare = st.multiselect("Select Tickers to Compare", tickers, default=[selected_ticker])
compare_df = df[df['Ticker'].isin(tickers_to_compare)]

kpi_chart = altair.Chart(compare_df).mark_line().encode(
    x='Date:T',
    y=altair.Y(f'{selected_kpi}:Q', title=selected_kpi.replace('_', ' ')),
    color='Ticker:N'
).properties(height=400)

st.altair_chart(kpi_chart, use_container_width=True)


# === Leaderboard Section ===
st.subheader("🏆 Top Performers Snapshot")

latest_df = df.sort_values('Date').groupby('Ticker').tail(1)

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Top 5 by 7-Day Momentum**")
    st.dataframe(latest_df.sort_values('Momentum_7d', ascending=False).head(5)[['Ticker', 'Momentum_7d']])

with col2:
    st.markdown("**Top 5 by Volatility**")
    st.dataframe(latest_df.sort_values('Volatility', ascending=False).head(5)[['Ticker', 'Volatility']])
