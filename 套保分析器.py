# -*- coding: utf-8 -*-
"""
第二步：套保必要性分析器 (Hedge Necessity Analyzer)
用于评估企业是否需要进行套期保值
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
from datetime import datetime
from scipy import stats
import warnings
import sys
import os

# 设置Windows控制台编码为UTF-8
if sys.platform == 'win32':
    try:
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        os.environ['PYTHONIOENCODING'] = 'utf-8'
    except:
        pass

warnings.filterwarnings('ignore')


class HedgeNecessityAnalyzer:
    """套保必要性分析器 - 评估套保的经济价值"""
    
    def __init__(
        self,
        spot_data: pd.Series,
        hedge_days: int,
        target_confidence: float = 0.95,
        cost_config: Optional[Dict] = None,
        futures_data: Optional[pd.DataFrame] = None,
        position_value: float = 1000000.0
    ):
        """
        初始化套保必要性分析器
        
        Args:
            spot_data: 归一化后的现货价格序列（pd.Series，索引为日期）
            hedge_days: 企业计划持仓的天数
            target_confidence: 置信水平（如0.95表示95%，0.99表示99%）
            cost_config: 成本配置字典
            futures_data: 期货数据（可选，用于基差风险分析）
            position_value: 持仓价值（默认100万）
        """
        self.spot_data = spot_data.sort_index()
        self.hedge_days = hedge_days
        self.target_confidence = target_confidence
        self.position_value = position_value
        self.futures_data = futures_data
        
        # 默认成本配置
        default_cost_config = {
            'commission_rate': 0.0002,  # 万分之二
            'financing_rate': 0.05,     # 5%年化利率
            'slippage_rate': 0.0001,    # 万分之一滑点
            'margin_rate': 0.1         # 10%保证金
        }
        
        self.cost_config = {**default_cost_config, **(cost_config or {})}
        
        # 分析结果存储
        self.volatility_analysis = {}
        self.cost_analysis = {}
        self.basis_risk_analysis = {}
        self.decision_result = {}
        
    def analyze(self) -> Dict:
        """执行完整的套保必要性分析"""
        # 1. 波动风险评估
        self.volatility_analysis = self._analyze_volatility_risk()
        
        # 2. 全项成本核算
        self.cost_analysis = self._analyze_costs()
        
        # 3. 基差风险分析（如果有期货数据）
        if self.futures_data is not None:
            self.basis_risk_analysis = self._analyze_basis_risk()
        
        # 4. 套保性价比判定
        self.decision_result = self._evaluate_hedge_efficiency()
        
        return {
            'volatility_analysis': self.volatility_analysis,
            'cost_analysis': self.cost_analysis,
            'basis_risk_analysis': self.basis_risk_analysis,
            'decision_result': self.decision_result
        }
    
    def _analyze_volatility_risk(self) -> Dict:
        """波动风险评估：计算年化波动率和VaR"""
        if len(self.spot_data) < 2:
            raise ValueError("现货数据不足，无法计算波动率")
        
        # 计算日收益率
        returns = self.spot_data.pct_change().dropna()
        
        if len(returns) < 30:
            warnings.warn("历史数据不足30天，波动率估算可能不准确")
        
        # 计算年化波动率（假设252个交易日）
        daily_volatility = returns.std()
        annualized_volatility = daily_volatility * np.sqrt(252)
        
        # 计算持仓期间的波动率
        holding_period_volatility = daily_volatility * np.sqrt(self.hedge_days)
        
        # 使用VaR模型计算最大亏损
        z_score = stats.norm.ppf(1 - self.target_confidence)
        var_percentage = abs(z_score * holding_period_volatility)
        
        # 转换为金额
        var_amount = var_percentage * self.position_value
        
        # 计算预期最大亏损（使用历史最差情况作为补充参考）
        worst_case_return = returns.min()
        worst_case_amount = abs(worst_case_return) * self.position_value
        
        return {
            'daily_volatility': daily_volatility,
            'annualized_volatility': annualized_volatility,
            'holding_period_volatility': holding_period_volatility,
            'var_percentage': var_percentage,
            'var_amount': var_amount,
            'confidence_level': self.target_confidence,
            'worst_case_return': worst_case_return,
            'worst_case_amount': worst_case_amount,
            'data_points': len(returns)
        }
    
    def _analyze_costs(self) -> Dict:
        """全项成本核算：交易成本、资金成本"""
        # 1. 交易成本
        commission_rate = self.cost_config['commission_rate']
        slippage_rate = self.cost_config['slippage_rate']
        
        # 开仓和平仓各一次
        commission_cost = self.position_value * commission_rate * 2
        slippage_cost = self.position_value * slippage_rate * 2
        
        total_trading_cost = commission_cost + slippage_cost
        
        # 2. 资金成本
        financing_rate = self.cost_config['financing_rate']
        margin_rate = self.cost_config['margin_rate']
        
        # 保证金占用
        margin_amount = self.position_value * margin_rate
        
        # 资金成本（按天数计算）
        days_per_year = 365
        financing_cost = margin_amount * financing_rate * (self.hedge_days / days_per_year)
        
        # 总成本
        total_cost = total_trading_cost + financing_cost
        
        return {
            'commission_cost': commission_cost,
            'slippage_cost': slippage_cost,
            'total_trading_cost': total_trading_cost,
            'margin_amount': margin_amount,
            'financing_cost': financing_cost,
            'total_cost': total_cost,
            'cost_percentage': total_cost / self.position_value
        }
    
    def _analyze_basis_risk(self) -> Dict:
        """基差风险分析"""
        if self.futures_data is None:
            return {'status': 'no_futures_data'}
        
        # 尝试从期货数据中提取基差
        basis_columns = [col for col in self.futures_data.columns if 'basis' in col.lower()]
        
        if not basis_columns:
            # 如果没有基差列，尝试计算
            spot_col = [col for col in self.futures_data.columns if 'spot' in col.lower()]
            futures_cols = [col for col in self.futures_data.columns if 'futures' in col.lower()]
            
            if spot_col and futures_cols:
                spot_series = self.futures_data[spot_col[0]].dropna()
                futures_series = self.futures_data[futures_cols[0]].dropna()
                
                # 对齐索引
                common_index = spot_series.index.intersection(futures_series.index)
                if len(common_index) == 0:
                    return {'status': 'no_common_dates'}
                
                spot_aligned = spot_series.loc[common_index]
                futures_aligned = futures_series.loc[common_index]
                basis_series = spot_aligned - futures_aligned
            else:
                return {'status': 'cannot_calculate_basis'}
        else:
            basis_series = self.futures_data[basis_columns[0]].dropna()
        
        if len(basis_series) < 30:
            return {'status': 'insufficient_data', 'data_points': len(basis_series)}
        
        # 计算基差的标准差和波动率
        basis_std = basis_series.std()
        basis_mean = basis_series.mean()
        
        # 使用基差绝对值的相对波动率
        if abs(basis_mean) > 0:
            basis_volatility = basis_std / abs(basis_mean)
        else:
            if hasattr(self, 'spot_data') and len(self.spot_data) > 0:
                spot_mean = self.spot_data.mean()
                basis_volatility = basis_std / abs(spot_mean) if spot_mean != 0 else float('inf')
            else:
                basis_volatility = float('inf')
        
        # 计算基差的年化波动率
        basis_returns = basis_series.pct_change().dropna()
        if len(basis_returns) > 0:
            basis_daily_vol = basis_returns.std()
            basis_annual_vol = basis_daily_vol * np.sqrt(252)
        else:
            basis_changes = basis_series.diff().dropna()
            if len(basis_changes) > 0:
                basis_daily_vol = basis_changes.std() / abs(basis_mean) if basis_mean != 0 else basis_changes.std()
                basis_annual_vol = basis_daily_vol * np.sqrt(252)
            else:
                basis_annual_vol = None
        
        # 判断基差风险等级
        if basis_volatility > 0.1:
            risk_level = 'high'
            risk_warning = "基差风险较高，可能抵消套保收益"
        elif basis_volatility > 0.05:
            risk_level = 'medium'
            risk_warning = "基差风险中等，需关注"
        else:
            risk_level = 'low'
            risk_warning = "基差风险较低"
        
        return {
            'status': 'success',
            'basis_mean': basis_mean,
            'basis_std': basis_std,
            'basis_volatility': basis_volatility,
            'basis_annual_vol': basis_annual_vol,
            'risk_level': risk_level,
            'risk_warning': risk_warning,
            'data_points': len(basis_series)
        }
    
    def _evaluate_hedge_efficiency(self) -> Dict:
        """套保性价比判定：计算风险成本比"""
        var_amount = self.volatility_analysis['var_amount']
        total_cost = self.cost_analysis['total_cost']
        
        # 计算风险成本比
        if total_cost > 0:
            risk_to_cost_ratio = var_amount / total_cost
        else:
            risk_to_cost_ratio = float('inf')
        
        # 决策逻辑
        if risk_to_cost_ratio > 2.0:
            recommendation = "强烈建议套保"
            recommendation_code = "STRONG_RECOMMEND"
            reason = "风险远大于成本，套保具有显著经济价值"
        elif risk_to_cost_ratio > 1.0:
            recommendation = "建议套保"
            recommendation_code = "RECOMMEND"
            reason = "对冲具备经济价值，风险略大于成本"
        else:
            recommendation = "不建议套保"
            recommendation_code = "NOT_RECOMMEND"
            reason = "对冲成本高于价格变动风险，建议敞口运行或缩短持仓期"
        
        return {
            'risk_to_cost_ratio': risk_to_cost_ratio,
            'recommendation': recommendation,
            'recommendation_code': recommendation_code,
            'reason': reason,
            'var_amount': var_amount,
            'total_cost': total_cost
        }
    
    def print_report(self):
        """打印决策简报"""
        print("\n" + "="*70)
        print("套保必要性分析报告")
        print("="*70)
        print(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # 基本信息
        print("【基本信息】")
        print(f"  持仓价值: {self.position_value:,.0f} 元")
        print(f"  持仓天数: {self.hedge_days} 天")
        print(f"  置信水平: {self.target_confidence*100:.1f}%")
        print()
        
        # 波动风险评估
        print("【波动风险评估】")
        vol_analysis = self.volatility_analysis
        print(f"  年化波动率: {vol_analysis['annualized_volatility']*100:.2f}%")
        print(f"  持仓期间波动率: {vol_analysis['holding_period_volatility']*100:.2f}%")
        print(f"  预期最大亏损 (VaR): {vol_analysis['var_percentage']*100:.2f}%")
        print(f"  预期最大亏损金额: {vol_analysis['var_amount']:,.0f} 元")
        print(f"  历史最差情况: {vol_analysis['worst_case_return']*100:.2f}% ({vol_analysis['worst_case_amount']:,.0f} 元)")
        print(f"  数据样本数: {vol_analysis['data_points']} 个交易日")
        print()
        
        # 成本分析
        print("【全项成本核算】")
        cost_analysis = self.cost_analysis
        print(f"  交易成本:")
        print(f"    - 手续费: {cost_analysis['commission_cost']:,.0f} 元")
        print(f"    - 滑点成本: {cost_analysis['slippage_cost']:,.0f} 元")
        print(f"    - 交易成本小计: {cost_analysis['total_trading_cost']:,.0f} 元")
        print(f"  资金成本:")
        print(f"    - 保证金占用: {cost_analysis['margin_amount']:,.0f} 元")
        print(f"    - 融资利息: {cost_analysis['financing_cost']:,.0f} 元")
        print(f"  总成本: {cost_analysis['total_cost']:,.0f} 元 ({cost_analysis['cost_percentage']*100:.4f}%)")
        print()
        
        # 基差风险分析
        if self.basis_risk_analysis and self.basis_risk_analysis.get('status') == 'success':
            print("【基差风险预警】")
            basis_risk = self.basis_risk_analysis
            print(f"  基差均值: {basis_risk['basis_mean']:.2f}")
            print(f"  基差标准差: {basis_risk['basis_std']:.2f}")
            print(f"  基差波动率: {basis_risk['basis_volatility']*100:.2f}%")
            if basis_risk.get('basis_annual_vol'):
                print(f"  基差年化波动率: {basis_risk['basis_annual_vol']*100:.2f}%")
            print(f"  风险等级: {basis_risk['risk_level'].upper()}")
            print(f"  {basis_risk['risk_warning']}")
            print()
        
        # 套保性价比判定
        print("【套保性价比判定】")
        decision = self.decision_result
        print(f"  风险成本比 (Risk-to-Cost Ratio): {decision['risk_to_cost_ratio']:.2f}")
        print(f"  预期最大亏损: {decision['var_amount']:,.0f} 元")
        print(f"  总对冲成本: {decision['total_cost']:,.0f} 元")
        print()
        print(f"  ════════════════════════════════════════════════════════")
        print(f"  决策建议: {decision['recommendation']}")
        print(f"  理由: {decision['reason']}")
        print(f"  ════════════════════════════════════════════════════════")
        print()
        
        print("="*70)
