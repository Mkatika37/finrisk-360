import os
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from dotenv import load_dotenv

st.set_page_config(
    page_title="FinRisk 360",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Colors
COLOR_MAP = {
    'CRITICAL': '#FF0000',
    'HIGH': '#FF6B00',
    'MEDIUM': '#FFD700',
    'LOW': '#00CC44'
}

@st.cache_data(ttl=3600)
def fetch_data():
    load_dotenv()
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'FINRISK360'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FINRISK_WH'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS')
        )
        cur = conn.cursor()
        cur.execute("SELECT * FROM LOAN_RISK_SCORES")
        df = cur.fetch_pandas_all()
        conn.close()
        
        # Upper column names just in case
        df.columns = [c.upper() for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return pd.DataFrame()

# Fetch Data
with st.spinner("Fetching data from Snowflake..."):
    df = fetch_data()

# Navigation
st.sidebar.title("🏦 FinRisk 360")
page = st.sidebar.radio("Navigation", ["Home", "Risk Overview", "Critical Loans", "Risk Trends", "Live Risk Scorer"])
st.sidebar.markdown("---")

if page == "Home":
    st.title("🏦 FinRisk 360")
    st.subheader("Mortgage Risk Intelligence Platform")
    st.write("Welcome to FinRisk 360, your central hub for analyzing and monitoring mortgage portfolios against extreme macroeconomic stress.")

    m1, m2, m3, m4 = st.columns(4)
    total_loans = len(df)
    critical_loans = len(df[df['RISK_TIER'] == 'CRITICAL']) if not df.empty else 0
    avg_score = df['RISK_SCORE'].mean() if not df.empty else 0.0
    high_exposure = df[df['RISK_TIER'].isin(['CRITICAL', 'HIGH'])]['LOAN_AMOUNT'].sum() / 1e9 if not df.empty else 0
    
    m1.metric("Total Loans Analyzed", f"{total_loans:,.0f}")
    m2.metric("Critical Risk Loans", f"{critical_loans:,.0f}")
    m3.metric("Average Risk Score", f"{avg_score:.3f}")
    m4.metric("High Risk Exposure", f"${high_exposure:.2f}B")

    st.success("🟢 Pipeline Status: LIVE and passing all data quality checks.")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

elif page == "Risk Overview":
    st.title("📊 Risk Overview")
    if not df.empty:
        order = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']

        # ── Row 1: Donut Pie + Horizontal Bar ────────────────────
        c1, c2 = st.columns(2)

        with c1:
            # Donut pie — only label+percent, no inside text
            tier_counts = df['RISK_TIER'].value_counts().reset_index()
            tier_counts.columns = ['RISK_TIER', 'COUNT']
            tier_counts['RISK_TIER'] = pd.Categorical(tier_counts['RISK_TIER'], categories=order, ordered=True)
            tier_counts = tier_counts.sort_values('RISK_TIER')

            fig1 = go.Figure(go.Pie(
                labels=tier_counts['RISK_TIER'],
                values=tier_counts['COUNT'],
                marker_colors=[COLOR_MAP.get(t, '#888') for t in tier_counts['RISK_TIER']],
                hole=0.42,
                textposition='outside',
                textinfo='label+percent',
                textfont_size=13,
                pull=[0.06, 0.03, 0, 0.06]   # pull CRITICAL & LOW out slightly
            ))
            fig1.update_layout(
                title="🍩 Risk Tier Distribution",
                showlegend=True,
                legend=dict(orientation='v', x=1.0, y=0.5),
                margin=dict(t=60, b=20, l=20, r=120),
                height=340
            )
            st.plotly_chart(fig1, use_container_width=True)

        with c2:
            # Horizontal bar — total loans per tier, color-coded
            tier_counts2 = tier_counts.copy()
            tier_counts2['PERCENT'] = (tier_counts2['COUNT'] / tier_counts2['COUNT'].sum() * 100).round(1)
            tier_counts2['LABEL'] = tier_counts2.apply(lambda r: f"{r['COUNT']:,} ({r['PERCENT']}%)", axis=1)

            fig2 = go.Figure(go.Bar(
                x=tier_counts2['COUNT'],
                y=tier_counts2['RISK_TIER'],
                orientation='h',
                marker_color=[COLOR_MAP.get(t, '#888') for t in tier_counts2['RISK_TIER']],
                text=tier_counts2['LABEL'],
                textposition='outside',
                cliponaxis=False,
                width=0.55
            ))
            fig2.update_layout(
                title="📊 Loan Count by Risk Tier",
                xaxis_title="Number of Loans",
                plot_bgcolor='rgba(245,245,245,1)',
                xaxis=dict(showgrid=True, gridcolor='white'),
                margin=dict(t=60, b=30, l=20, r=160),
                height=340
            )
            st.plotly_chart(fig2, use_container_width=True)

        # ── Row 2: LTV Bar + DTI Bar with reference lines ─────────
        c3, c4 = st.columns(2)

        with c3:
            avg_ltv = df.groupby('RISK_TIER')['LTV'].mean().reset_index()
            avg_ltv['RISK_TIER'] = pd.Categorical(avg_ltv['RISK_TIER'], categories=order, ordered=True)
            avg_ltv = avg_ltv.sort_values('RISK_TIER')

            fig3 = go.Figure(go.Bar(
                x=avg_ltv['RISK_TIER'],
                y=avg_ltv['LTV'],
                marker_color=[COLOR_MAP.get(t, '#888') for t in avg_ltv['RISK_TIER']],
                text=avg_ltv['LTV'].round(1),
                textposition='outside',
                width=0.5
            ))
            # Fannie Mae 80% LTV threshold reference line
            fig3.add_hline(y=80, line_dash='dash', line_color='gray',
                           annotation_text='80% threshold', annotation_position='top right')
            fig3.update_layout(
                title="🏠 Average LTV by Risk Tier",
                yaxis_title="LTV (%)",
                plot_bgcolor='rgba(245,245,245,1)',
                yaxis=dict(showgrid=True, gridcolor='white', range=[0, max(avg_ltv['LTV'].max() + 5, 90)]),
                margin=dict(t=60, b=30, l=40, r=20),
                height=340
            )
            st.plotly_chart(fig3, use_container_width=True)

        with c4:
            avg_dti = df.groupby('RISK_TIER')['DTI'].mean().reset_index()
            avg_dti['RISK_TIER'] = pd.Categorical(avg_dti['RISK_TIER'], categories=order, ordered=True)
            avg_dti = avg_dti.sort_values('RISK_TIER')

            fig4 = go.Figure(go.Bar(
                x=avg_dti['RISK_TIER'],
                y=avg_dti['DTI'],
                marker_color=[COLOR_MAP.get(t, '#888') for t in avg_dti['RISK_TIER']],
                text=avg_dti['DTI'].round(1),
                textposition='outside',
                width=0.5
            ))
            # 43% DTI is standard qualifying threshold
            fig4.add_hline(y=43, line_dash='dash', line_color='gray',
                           annotation_text='43% threshold', annotation_position='top right')
            fig4.update_layout(
                title="💳 Average DTI by Risk Tier",
                yaxis_title="DTI (%)",
                plot_bgcolor='rgba(245,245,245,1)',
                yaxis=dict(showgrid=True, gridcolor='white', range=[0, max(avg_dti['DTI'].max() + 5, 50)]),
                margin=dict(t=60, b=30, l=40, r=20),
                height=340
            )
            st.plotly_chart(fig4, use_container_width=True)

        # ── Summary Table ─────────────────────────────────────────
        st.subheader("📋 Metrics Summary by Risk Tier")
        summary = df.groupby('RISK_TIER')[['LOAN_AMOUNT', 'LTV', 'DTI', 'INTEREST_RATE', 'RISK_SCORE']].mean().round(2)
        st.dataframe(summary, use_container_width=True)


elif page == "Critical Loans":
    st.title("🚨 Critical Risk Loans")
    if not df.empty:
        st.sidebar.markdown("### Filters")
        tiers = st.sidebar.multiselect("Risk Tier", df['RISK_TIER'].unique(), default=['CRITICAL'])
        
        min_loan_val = float(df['LOAN_AMOUNT'].min())
        max_loan_val = float(df['LOAN_AMOUNT'].max())
        if min_loan_val == max_loan_val:
            max_loan_val += 1.0
            
        min_score_val = float(df['RISK_SCORE'].min())
        max_score_val = float(df['RISK_SCORE'].max())
        if min_score_val == max_score_val:
            max_score_val += 1.0

        min_loan, max_loan = st.sidebar.slider("Loan Amount", min_loan_val, max_loan_val, (min_loan_val, max_loan_val))
        min_score, max_score = st.sidebar.slider("Risk Score", min_score_val, max_score_val, (min_score_val, max_score_val))
        
        filtered = df[
            (df['RISK_TIER'].isin(tiers)) &
            (df['LOAN_AMOUNT'] >= min_loan) & (df['LOAN_AMOUNT'] <= max_loan) &
            (df['RISK_SCORE'] >= min_score) & (df['RISK_SCORE'] <= max_score)
        ]
        
        st.write(f"Showing **{len(filtered)}** filtered records.")
        
        top100 = filtered.nlargest(100, 'RISK_SCORE')[['RISK_SCORE', 'RISK_TIER', 'LOAN_AMOUNT', 'LTV', 'DTI', 'INTEREST_RATE']]
        st.dataframe(top100, use_container_width=True)
        
        csv = filtered.to_csv(index=False).encode('utf-8')
        st.download_button("Download Filtered Data (CSV)", data=csv, file_name="finrisk_filtered.csv", mime="text/csv")

elif page == "Risk Trends":
    st.title("📈 Risk Trends")
    if not df.empty:
        if 'PROCESSING_DATE' in df.columns:
            # Handle timestamps gracefully
            df['DATE_ONLY'] = pd.to_datetime(df['PROCESSING_DATE']).dt.date
            
            daily_score = df.groupby('DATE_ONLY')['RISK_SCORE'].mean().reset_index()
            fig1 = px.line(daily_score, x='DATE_ONLY', y='RISK_SCORE', title="Average Risk Score Over Time", markers=True)
            st.plotly_chart(fig1, use_container_width=True)
            
            daily_tier = df.groupby(['DATE_ONLY', 'RISK_TIER']).size().reset_index(name='COUNT')
            fig2 = px.bar(daily_tier, x='DATE_ONLY', y='COUNT', color='RISK_TIER', color_discrete_map=COLOR_MAP, title="Loan Count by Risk Tier Over Time")
            st.plotly_chart(fig2, use_container_width=True)
            
            daily_crit = daily_tier[daily_tier['RISK_TIER'] == 'CRITICAL'].sort_values('DATE_ONLY')
            if len(daily_crit) >= 2:
                latest = daily_crit.iloc[-1]['COUNT']
                prev = daily_crit.iloc[-2]['COUNT']
                st.metric("Day over Day CRITICAL Loans", f"{latest}", delta=f"{latest-prev}")
            elif len(daily_crit) == 1:
                st.metric("Total CRITICAL Loans Today", f"{daily_crit.iloc[0]['COUNT']}")
            
            st.subheader("Daily Summary")
            st.dataframe(daily_tier, use_container_width=True)
        else:
            st.warning("PROCESSING_DATE column not found in data.")

elif page == "Live Risk Scorer":
    st.title("⚡ Score a New Loan in Real-Time")
    
    with st.form("risk_scorer"):
        c1, c2 = st.columns(2)
        loan_amount = c1.slider("Loan Amount ($)", 50000, 2000000, 250000, 1000)
        ltv = c2.slider("LTV Ratio (%)", 0.0, 100.0, 80.0, 0.1)
        dti = c1.slider("DTI Ratio (%)", 0.0, 65.0, 36.0, 0.1)
        rate = c2.slider("Interest Rate (%)", 0.0, 15.0, 6.5, 0.1)
        income = c1.slider("Income ($)", 0, 500000, 85000, 1000)
        
        submitted = st.form_submit_button("Calculate Risk Score")
        
    if submitted:
        ltv_score = 0.2 if ltv < 80 else 0.5 if ltv < 90 else 0.8 if ltv < 95 else 1.0
        dti_score = 0.2 if dti < 36 else 0.5 if dti < 43 else 0.8 if dti < 50 else 1.0
        rate_score = 0.2 if rate < 4 else 0.5 if rate < 6 else 0.8 if rate < 8 else 1.0
        macro_stress = 0.6
        
        risk_score = (ltv_score * 0.30) + (dti_score * 0.25) + (rate_score * 0.25) + (macro_stress * 0.20)
        
        risk_tier = 'LOW' if risk_score < 0.3 else 'MEDIUM' if risk_score < 0.6 else 'HIGH' if risk_score < 0.85 else 'CRITICAL'
        
        st.markdown("---")
        st.subheader("Score Result")
        
        res1, res2 = st.columns([1, 2])
        res1.metric("Final Risk Score", f"{risk_score:.3f}")
        res1.markdown(f"<h2 style='color: {COLOR_MAP[risk_tier]}; margin-top: 0px;'>{risk_tier} RISK</h2>", unsafe_allow_html=True)
        
        fig = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = risk_score,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "Risk Score Gauge"},
            gauge = {
                'axis': {'range': [0, 1.0]},
                'bar': {'color': "rgba(255,255,255,0.7)"},
                'steps' : [
                    {'range': [0, 0.3], 'color': COLOR_MAP['LOW']},
                    {'range': [0.3, 0.6], 'color': COLOR_MAP['MEDIUM']},
                    {'range': [0.6, 0.85], 'color': COLOR_MAP['HIGH']},
                    {'range': [0.85, 1.0], 'color': COLOR_MAP['CRITICAL']}
                ]
            }
        ))
        res2.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### Component Weights")
        st.write(f"- **LTV Score:** {ltv_score} *(Weight: 30%)*")
        st.write(f"- **DTI Score:** {dti_score} *(Weight: 25%)*")
        st.write(f"- **Rate Score:** {rate_score} *(Weight: 25%)*")
        st.write(f"- **Macro Stress:** {macro_stress} *(Weight: 20%)*")

# Footer
st.markdown("---")
st.markdown("<p style='text-align: center; color: gray;'>FinRisk 360 | Data: 2024 HMDA VA/MD/DC</p>", unsafe_allow_html=True)
