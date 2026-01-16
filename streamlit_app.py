# -*- coding: utf-8 -*-
"""
Streamlit 应用 - OmniHedge 套保必要性分析系统
交互式Web界面，方便用户配置参数并查看分析结果
"""

import streamlit as st
import sys
import os
import pickle
import pandas as pd
import importlib.util

# 设置页面配置
st.set_page_config(
    page_title="OmniHedge 套保分析系统",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 设置编码（Windows环境）
if sys.platform == 'win32':
    try:
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        os.environ['PYTHONIOENCODING'] = 'utf-8'
    except:
        pass

# 动态导入中文文件名的模块
def import_module_by_name(module_name, file_path):
    """动态导入模块（支持中文文件名）"""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# 导入模块
current_dir = os.path.dirname(os.path.abspath(__file__))

try:
    data_gateway_module = import_module_by_name('数据网关', os.path.join(current_dir, '数据网关.py'))
    OmniDataGateway = data_gateway_module.OmniDataGateway
    
    analyzer_module = import_module_by_name('套保分析器', os.path.join(current_dir, '套保分析器.py'))
    HedgeNecessityAnalyzer = analyzer_module.HedgeNecessityAnalyzer
except Exception as e:
    st.error(f"模块导入失败: {e}")
    st.stop()


# ============================================================
# 主应用界面
# ============================================================

st.title("📊 OmniHedge 套保必要性分析系统")
st.markdown("---")

# 侧边栏：参数配置
with st.sidebar:
    st.header("⚙️ 参数配置")
    
    # 数据文件上传
    st.subheader("📁 数据文件")
    data_file = st.file_uploader(
        "上传预处理数据文件 (processed_data.pkl)",
        type=['pkl'],
        help="如果还没有预处理数据，请先运行 数据预处理.py"
    )
    
    # 或者使用本地文件
    use_local_file = st.checkbox("使用本地文件", value=True)
    if use_local_file:
        local_file_path = st.text_input(
            "本地文件路径",
            value="processed_data.pkl",
            help="相对于当前目录的文件路径"
        )
    
    st.markdown("---")
    
    # 企业持仓参数
    st.subheader("💼 企业持仓参数")
    hedge_days = st.slider(
        "计划持仓天数",
        min_value=1,
        max_value=365,
        value=7,
        step=1,
        help="企业计划持有现货的天数"
    )
    
    target_confidence = st.select_slider(
        "置信水平",
        options=[0.90, 0.95, 0.99],
        value=0.95,
        help="VaR计算的置信水平"
    )
    
    position_value = st.number_input(
        "持仓价值 (元)",
        min_value=10000.0,
        max_value=100000000.0,
        value=1000000.0,
        step=100000.0,
        format="%.0f",
        help="现货持仓的总价值"
    )
    
    st.markdown("---")
    
    # 成本配置
    st.subheader("💰 成本配置")
    commission_rate = st.number_input(
        "手续费率",
        min_value=0.0,
        max_value=0.01,
        value=0.0002,
        step=0.0001,
        format="%.4f",
        help="手续费率，0.0002表示万分之二"
    )
    
    financing_rate = st.number_input(
        "融资利率 (年化)",
        min_value=0.0,
        max_value=0.2,
        value=0.05,
        step=0.01,
        format="%.2f",
        help="融资利率，0.05表示5%年化"
    )
    
    slippage_rate = st.number_input(
        "滑点率",
        min_value=0.0,
        max_value=0.01,
        value=0.0001,
        step=0.0001,
        format="%.4f",
        help="滑点率，0.0001表示万分之一"
    )
    
    margin_rate = st.number_input(
        "保证金比例",
        min_value=0.05,
        max_value=0.5,
        value=0.1,
        step=0.01,
        format="%.2f",
        help="保证金比例，0.1表示10%"
    )


# 主内容区域
tab1, tab2, tab3 = st.tabs(["📈 分析结果", "📋 数据概览", "ℹ️ 使用说明"])

with tab1:
    st.header("套保必要性分析结果")
    
    # 加载数据
    processed_data = None
    
    if use_local_file and os.path.exists(local_file_path):
        try:
            with open(local_file_path, 'rb') as f:
                processed_data = pickle.load(f)
            st.success(f"✓ 成功加载本地数据文件: {local_file_path}")
        except Exception as e:
            st.error(f"加载本地文件失败: {e}")
    elif data_file is not None:
        try:
            processed_data = pickle.load(data_file)
            st.success("✓ 成功加载上传的数据文件")
        except Exception as e:
            st.error(f"加载上传文件失败: {e}")
    else:
        st.warning("⚠️ 请上传数据文件或使用本地文件")
        st.info("💡 提示：如果还没有预处理数据，请先运行 `数据预处理.py` 生成 processed_data.pkl 文件")
    
    # 执行分析
    if processed_data is not None:
        try:
            panel = processed_data['panel']
            contract_info = processed_data['contract_info']
            
            # 显示数据基本信息
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("数据面板形状", f"{panel.shape[0]} 行 × {panel.shape[1]} 列")
            with col2:
                st.metric("日期范围", f"{panel.index.min().date()} 至 {panel.index.max().date()}")
            with col3:
                st.metric("合约数量", len(contract_info))
            
            # 提取现货价格序列
            spot_col = [col for col in panel.columns if 'spot' in col.lower()]
            if not spot_col:
                st.error("❌ 未找到现货价格列")
            else:
                spot_data = panel[spot_col[0]].dropna()
                
                if len(spot_data) < 30:
                    st.warning(f"⚠️ 现货数据不足（仅{len(spot_data)}个数据点），建议至少30个数据点")
                else:
                    # 配置成本参数
                    cost_config = {
                        'commission_rate': commission_rate,
                        'financing_rate': financing_rate,
                        'slippage_rate': slippage_rate,
                        'margin_rate': margin_rate
                    }
                    
                    # 执行分析
                    if st.button("🚀 开始分析", type="primary", use_container_width=True):
                        with st.spinner("正在分析中，请稍候..."):
                            analyzer = HedgeNecessityAnalyzer(
                                spot_data=spot_data,
                                hedge_days=hedge_days,
                                target_confidence=target_confidence,
                                cost_config=cost_config,
                                futures_data=panel,
                                position_value=position_value
                            )
                            
                            results = analyzer.analyze()
                            
                            # 显示分析结果
                            st.markdown("### 📊 波动风险评估")
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric(
                                    "年化波动率",
                                    f"{results['volatility_analysis']['annualized_volatility']*100:.2f}%"
                                )
                            with col2:
                                st.metric(
                                    "持仓期间波动率",
                                    f"{results['volatility_analysis']['holding_period_volatility']*100:.2f}%"
                                )
                            with col3:
                                st.metric(
                                    "预期最大亏损 (VaR)",
                                    f"{results['volatility_analysis']['var_percentage']*100:.2f}%"
                                )
                            with col4:
                                st.metric(
                                    "预期最大亏损金额",
                                    f"{results['volatility_analysis']['var_amount']:,.0f} 元"
                                )
                            
                            st.markdown("### 💰 成本分析")
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric(
                                    "交易成本",
                                    f"{results['cost_analysis']['total_trading_cost']:,.0f} 元"
                                )
                            with col2:
                                st.metric(
                                    "资金成本",
                                    f"{results['cost_analysis']['financing_cost']:,.0f} 元"
                                )
                            with col3:
                                st.metric(
                                    "总成本",
                                    f"{results['cost_analysis']['total_cost']:,.0f} 元"
                                )
                            with col4:
                                st.metric(
                                    "成本占比",
                                    f"{results['cost_analysis']['cost_percentage']*100:.4f}%"
                                )
                            
                            # 基差风险分析
                            if results['basis_risk_analysis'].get('status') == 'success':
                                st.markdown("### ⚠️ 基差风险预警")
                                basis_risk = results['basis_risk_analysis']
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("基差波动率", f"{basis_risk['basis_volatility']*100:.2f}%")
                                with col2:
                                    risk_level = basis_risk['risk_level']
                                    if risk_level == 'high':
                                        st.error(f"风险等级: {risk_level.upper()}")
                                    elif risk_level == 'medium':
                                        st.warning(f"风险等级: {risk_level.upper()}")
                                    else:
                                        st.success(f"风险等级: {risk_level.upper()}")
                                with col3:
                                    st.info(basis_risk['risk_warning'])
                            
                            # 决策建议
                            st.markdown("### 🎯 套保性价比判定")
                            decision = results['decision_result']
                            
                            # 风险成本比可视化
                            ratio = decision['risk_to_cost_ratio']
                            st.metric(
                                "风险成本比 (Risk-to-Cost Ratio)",
                                f"{ratio:.2f}",
                                delta=f"预期亏损: {decision['var_amount']:,.0f} 元 | 总成本: {decision['total_cost']:,.0f} 元"
                            )
                            
                            # 决策建议
                            recommendation = decision['recommendation']
                            reason = decision['reason']
                            
                            if ratio > 2.0:
                                st.success(f"## ✅ {recommendation}")
                            elif ratio > 1.0:
                                st.info(f"## 💡 {recommendation}")
                            else:
                                st.warning(f"## ⚠️ {recommendation}")
                            
                            st.info(f"**理由：** {reason}")
                            
                            # 详细数据表格
                            with st.expander("📋 查看详细数据"):
                                summary_data = {
                                    '指标': [
                                        '年化波动率',
                                        '持仓期间波动率',
                                        '预期最大亏损(VaR)',
                                        '预期最大亏损金额',
                                        '交易成本',
                                        '资金成本',
                                        '总成本',
                                        '风险成本比',
                                        '决策建议'
                                    ],
                                    '数值': [
                                        f"{results['volatility_analysis']['annualized_volatility']*100:.2f}%",
                                        f"{results['volatility_analysis']['holding_period_volatility']*100:.2f}%",
                                        f"{results['volatility_analysis']['var_percentage']*100:.2f}%",
                                        f"{results['volatility_analysis']['var_amount']:,.0f} 元",
                                        f"{results['cost_analysis']['total_trading_cost']:,.0f} 元",
                                        f"{results['cost_analysis']['financing_cost']:,.0f} 元",
                                        f"{results['cost_analysis']['total_cost']:,.0f} 元",
                                        f"{results['decision_result']['risk_to_cost_ratio']:.2f}",
                                        results['decision_result']['recommendation']
                                    ]
                                }
                                summary_df = pd.DataFrame(summary_data)
                                st.dataframe(summary_df, use_container_width=True)
                                
                                # 下载按钮
                                csv = summary_df.to_csv(index=False, encoding='utf-8-sig')
                                st.download_button(
                                    label="📥 下载分析结果 (CSV)",
                                    data=csv,
                                    file_name="hedge_analysis_summary.csv",
                                    mime="text/csv"
                                )
        
        except Exception as e:
            st.error(f"分析过程出错: {e}")
            st.exception(e)

with tab2:
    st.header("数据概览")
    
    if processed_data is not None:
        panel = processed_data['panel']
        contract_info = processed_data['contract_info']
        
        # 数据面板预览
        st.subheader("数据面板预览")
        st.dataframe(panel.head(100), use_container_width=True)
        
        # 合约信息
        st.subheader("合约信息")
        if contract_info:
            contract_df = pd.DataFrame(contract_info).T
            st.dataframe(contract_df, use_container_width=True)
    else:
        st.info("请先加载数据文件")

with tab3:
    st.header("使用说明")
    
    st.markdown("""
    ## 📖 OmniHedge 套保分析系统使用指南
    
    ### 第一步：数据预处理
    
    在使用本系统之前，需要先运行数据预处理脚本：
    
    ```bash
    python 数据预处理.py
    ```
    
    这会生成 `processed_data.pkl` 文件，包含所有处理好的数据。
    
    ### 第二步：运行 Streamlit 应用
    
    ```bash
    streamlit run streamlit_app.py
    ```
    
    ### 第三步：配置参数
    
    在左侧边栏中配置以下参数：
    
    - **计划持仓天数**：企业计划持有现货的天数
    - **置信水平**：VaR计算的置信水平（90%、95%、99%）
    - **持仓价值**：现货持仓的总价值（单位：元）
    - **手续费率**：期货交易手续费率
    - **融资利率**：年化融资利率
    - **滑点率**：交易滑点率
    - **保证金比例**：期货保证金比例
    
    ### 第四步：查看分析结果
    
    点击"开始分析"按钮后，系统会：
    
    1. 计算波动风险评估（年化波动率、VaR等）
    2. 核算全项成本（交易成本、资金成本）
    3. 分析基差风险
    4. 给出套保建议
    
    ### 决策建议说明
    
    - **风险成本比 > 2.0**：强烈建议套保（风险远大于成本）
    - **风险成本比 1.0-2.0**：建议套保（对冲具备经济价值）
    - **风险成本比 < 1.0**：不建议套保（成本高于风险）
    
    ### 注意事项
    
    1. 确保数据文件路径正确
    2. 现货数据至少需要30个交易日
    3. 所有参数都可以根据实际情况调整
    4. 分析结果可以下载为CSV文件
    
    ### 技术支持
    
    如有问题，请查看项目 README.md 文件或提交 Issue。
    """)
