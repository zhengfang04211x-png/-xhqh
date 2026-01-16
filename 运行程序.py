# -*- coding: utf-8 -*-
"""
运行主程序
使用预处理好的数据进行分析
"""

import sys
import os
import pickle
import pandas as pd
import importlib.util

# 设置编码
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

# 导入套保分析器模块
current_dir = os.path.dirname(os.path.abspath(__file__))
analyzer_module = import_module_by_name('套保分析器', os.path.join(current_dir, '套保分析器.py'))
HedgeNecessityAnalyzer = analyzer_module.HedgeNecessityAnalyzer


# ============================================================
# 参数配置区域 - 可以在这里修改测试参数
# ============================================================

# 预处理数据文件（如果文件不存在，请先运行 数据预处理.py）
PROCESSED_DATA_FILE = "processed_data.pkl"

# 企业持仓参数
HEDGE_DAYS = 7              # 计划持仓天数（可以修改为：7, 15, 30, 60, 90等）
TARGET_CONFIDENCE = 0.95     # 置信水平（可以修改为：0.90, 0.95, 0.99等）
POSITION_VALUE = 1000000.0   # 持仓价值（单位：元，可以修改为：500000, 1000000, 5000000等）

# 成本配置参数（可以根据实际情况修改）
COST_CONFIG = {
    'commission_rate': 0.0002,  # 手续费率：0.0002表示万分之二（可以修改为：0.0001, 0.0003等）
    'financing_rate': 0.05,     # 融资利率：0.05表示5%年化（可以修改为：0.03, 0.06, 0.08等）
    'slippage_rate': 0.0001,    # 滑点率：0.0001表示万分之一（可以修改为：0.00005, 0.0002等）
    'margin_rate': 0.1          # 保证金比例：0.1表示10%（可以修改为：0.08, 0.12, 0.15等）
}

# ============================================================


def load_processed_data(data_file):
    """加载预处理好的数据"""
    if not os.path.exists(data_file):
        print(f"❌ 错误: 预处理数据文件不存在: {data_file}")
        print(f"\n请先运行数据预处理脚本:")
        print(f"  python 数据预处理.py")
        return None
    
    print(f"正在加载预处理数据: {data_file}")
    with open(data_file, 'rb') as f:
        processed_data = pickle.load(f)
    
    return processed_data


def main():
    print("="*70)
    print("OmniHedge 系统 - 套保必要性分析")
    print("="*70)
    
    # ========== 第一步：加载预处理数据 ==========
    print("\n【第一步】加载预处理数据...")
    processed_data = load_processed_data(PROCESSED_DATA_FILE)
    
    if processed_data is None:
        return
    
    panel = processed_data['panel']
    contract_info = processed_data['contract_info']
    
    print(f"  数据面板形状: {panel.shape}")
    print(f"  日期范围: {panel.index.min()} 至 {panel.index.max()}")
    print(f"  合约数量: {len(contract_info)}")
    
    # 提取现货价格序列
    spot_col = [col for col in panel.columns if 'spot' in col.lower()]
    if not spot_col:
        print("\n❌ 错误: 未找到现货价格列")
        return
    
    spot_data = panel[spot_col[0]].dropna()
    
    if len(spot_data) < 30:
        print(f"\n⚠ 警告: 现货数据不足（仅{len(spot_data)}个数据点），建议至少30个数据点")
        return
    
    print(f"\n现货数据: {len(spot_data)} 个交易日")
    
    # ========== 第二步：配置套保分析参数 ==========
    print("\n【第二步】配置套保分析参数...")
    print(f"  持仓天数: {HEDGE_DAYS} 天")
    print(f"  置信水平: {TARGET_CONFIDENCE*100:.0f}%")
    print(f"  持仓价值: {POSITION_VALUE:,.0f} 元")
    print(f"  手续费率: {COST_CONFIG['commission_rate']*10000:.2f} 万分之一")
    print(f"  融资利率: {COST_CONFIG['financing_rate']*100:.2f}%")
    print(f"  滑点率: {COST_CONFIG['slippage_rate']*10000:.2f} 万分之一")
    print(f"  保证金比例: {COST_CONFIG['margin_rate']*100:.0f}%")
    
    # ========== 第三步：执行套保必要性分析 ==========
    print("\n【第三步】执行套保必要性分析...")
    
    analyzer = HedgeNecessityAnalyzer(
        spot_data=spot_data,
        hedge_days=HEDGE_DAYS,
        target_confidence=TARGET_CONFIDENCE,
        cost_config=COST_CONFIG,
        futures_data=panel,  # 传入统一面板用于基差分析
        position_value=POSITION_VALUE
    )
    
    # 执行分析
    results = analyzer.analyze()
    
    # ========== 第四步：输出决策报告 ==========
    print("\n【第四步】生成决策报告...")
    analyzer.print_report()
    
    # ========== 第五步：保存结果（可选） ==========
    print("\n【第五步】保存分析结果...")
    
    # 将结果保存为CSV
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
    summary_file = "hedge_analysis_summary.csv"
    summary_df.to_csv(summary_file, index=False, encoding='utf-8-sig')
    print(f"  ✓ 分析摘要已保存到: {summary_file}")
    
    print("\n" + "="*70)
    print("分析完成！")
    print("="*70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n\n❌ 运行错误: {e}")
        import traceback
        traceback.print_exc()
