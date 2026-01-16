# -*- coding: utf-8 -*-
"""
第一步：全自动化数据网关 (OmniDataGateway)
自动识别、清洗和对齐现货与期货数据
"""

import os
import re
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import warnings

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


class OmniDataGateway:
    """全自动化数据网关 - 自动识别、清洗和对齐金融数据"""
    
    def __init__(self, data_dir: str = "."):
        """
        初始化数据网关
        
        Args:
            data_dir: 数据文件所在目录
        """
        self.data_dir = Path(data_dir)
        self.spot_data = None
        self.futures_data = {}  # {contract_code: DataFrame}
        self.contract_info = {}
        self.quality_report = {}
        self._field_mapping_cache = {}
        
        # 标准字段映射模式（支持中文列名）
        self.STANDARD_FIELDS = {
            'date': ['date', '日期', '时间', '交易日期', 'tradingday', 'time', 't_date'],
            'price': ['price', '收盘价', '结算价', '现货价格', 'close', 'lastprice', 'settlement'],
            'oi': ['oi', '持仓量', 'openinterest'],
            'volume': ['volume', '成交量', 'vol']
        }
        
    def scan_and_load(self, recursive: bool = True) -> Dict[str, int]:
        """
        扫描并加载指定目录下的所有CSV文件
        
        Returns:
            统计信息 {spot_count, futures_count, errors}
        """
        stats = {'spot_count': 0, 'futures_count': 0, 'errors': []}
        
        if not self.data_dir.exists():
            raise FileNotFoundError(f"数据目录不存在: {self.data_dir}")
        
        # 扫描所有CSV文件
        if recursive:
            csv_files = list(self.data_dir.rglob("*.csv"))
        else:
            csv_files = list(self.data_dir.glob("*.csv"))
        
        # 排除备份文件
        csv_files = [f for f in csv_files if not f.name.endswith('.bak')]
        
        if len(csv_files) == 0:
            warnings.warn(f"在 {self.data_dir} 中未找到任何CSV文件")
            return stats
        
        print(f"发现 {len(csv_files)} 个CSV文件，开始处理...")
        
        for csv_file in csv_files:
            try:
                # 尝试多种编码读取文件
                df = None
                encodings = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'gb18030']
                for enc in encodings:
                    try:
                        df = pd.read_csv(csv_file, encoding=enc)
                        if df is not None and not df.empty:
                            break
                    except (UnicodeDecodeError, UnicodeError):
                        continue
                    except Exception:
                        continue
                
                if df is None or df.empty:
                    stats['errors'].append(f"{csv_file.name}: 文件为空或无法读取")
                    continue
                
                # 自动识别数据类型
                data_type = self._detect_data_type(df, csv_file.name)
                
                if data_type == 'spot':
                    self.spot_data = self._normalize_dataframe(df, 'spot', csv_file.stem)
                    stats['spot_count'] += 1
                    print(f"  ✓ 识别为现货: {csv_file.name}")
                    
                elif data_type == 'futures':
                    contract_code = self._extract_contract_code(csv_file.name, df)
                    normalized_df = self._normalize_dataframe(df, 'futures', contract_code)
                    self.futures_data[contract_code] = normalized_df
                    stats['futures_count'] += 1
                    print(f"  ✓ 识别为期货: {csv_file.name} -> {contract_code}")
                    
                else:
                    stats['errors'].append(f"{csv_file.name}: 无法识别数据类型")
                    
            except Exception as e:
                stats['errors'].append(f"{csv_file.name}: {str(e)}")
                print(f"  ✗ 处理失败: {csv_file.name} - {str(e)}")
        
        print(f"\n扫描完成: 现货 {stats['spot_count']} 个, 期货 {stats['futures_count']} 个")
        if stats['errors']:
            print(f"处理错误: {len(stats['errors'])} 个")
        
        return stats
    
    def _detect_data_type(self, df: pd.DataFrame, filename: str) -> Optional[str]:
        """自动识别数据类型（现货或期货）"""
        columns_lower = [col.lower().strip() for col in df.columns]
        
        # 检查是否包含持仓量字段（期货特征）
        has_oi = any(
            re.search(r'(持仓量|openinterest|oi)', col, re.IGNORECASE) 
            for col in columns_lower
        )
        
        # 检查是否包含成交量字段
        has_volume = any(
            re.search(r'(成交量|volume|vol)', col, re.IGNORECASE) 
            for col in columns_lower
        )
        
        # 检查是否包含现货价格字段
        has_spot_price = any(
            re.search(r'(现货价格|spot)', col, re.IGNORECASE) 
            for col in columns_lower
        )
        
        # 期货识别逻辑：有持仓量或（有成交量且无现货价格）
        if has_oi or (has_volume and not has_spot_price):
            if has_oi:
                return 'futures'
            # 检查数据跨度
            date_col = self._map_standard_field(df.columns, 'date')
            if date_col:
                try:
                    dates = pd.to_datetime(df[date_col], errors='coerce')
                    if not dates.empty:
                        date_span = (dates.max() - dates.min()).days
                        if date_span < 1000:
                            return 'futures'
                except:
                    pass
        
        # 现货识别逻辑
        if has_spot_price and not has_oi:
            return 'spot'
        
        # 现货识别：无持仓量且数据跨度长
        if not has_oi and len(df) > 100:
            date_col = self._map_standard_field(df.columns, 'date')
            if date_col:
                try:
                    dates = pd.to_datetime(df[date_col], errors='coerce')
                    if not dates.empty:
                        date_span = (dates.max() - dates.min()).days
                        if date_span > 1000:
                            return 'spot'
                except:
                    pass
        
        return None
    
    def _map_standard_field(self, columns: List[str], standard_field: str) -> Optional[str]:
        """使用正则表达式将各种可能的列名映射到标准字段"""
        cache_key = (tuple(columns), standard_field)
        if cache_key in self._field_mapping_cache:
            return self._field_mapping_cache[cache_key]
        
        patterns = self.STANDARD_FIELDS.get(standard_field, [])
        
        for col in columns:
            col_lower = col.lower().strip()
            for pattern in patterns:
                if re.search(pattern, col_lower, re.IGNORECASE):
                    self._field_mapping_cache[cache_key] = col
                    return col
        
        self._field_mapping_cache[cache_key] = None
        return None
    
    def _normalize_dataframe(self, df: pd.DataFrame, data_type: str, identifier: str) -> pd.DataFrame:
        """归一化数据框：统一字段名、格式转换、数据清洗"""
        df_normalized = df.copy()
        
        # 映射日期字段
        date_col = self._map_standard_field(df.columns, 'date')
        if not date_col:
            raise ValueError(f"无法找到日期字段: {identifier}")
        
        # 转换日期格式
        date_series = pd.to_datetime(df[date_col], errors='coerce', infer_datetime_format=True)
        
        if date_series.isna().all():
            date_str = df[date_col].astype(str)
            date_series = pd.to_datetime(
                date_str.str.replace(r'(\d{4})(\d{2})(\d{2})', r'\1-\2-\3', regex=True), 
                errors='coerce'
            )
        
        df_normalized['date'] = date_series
        
        # 映射价格字段
        price_col = self._map_standard_field(df.columns, 'price')
        if price_col:
            df_normalized['price'] = pd.to_numeric(df[price_col], errors='coerce')
        else:
            if '收盘价' in df.columns:
                df_normalized['price'] = pd.to_numeric(df['收盘价'], errors='coerce')
            elif '现货价格' in df.columns:
                df_normalized['price'] = pd.to_numeric(df['现货价格'], errors='coerce')
            else:
                raise ValueError(f"无法找到价格字段: {identifier}")
        
        # 映射期货特有字段
        if data_type == 'futures':
            oi_col = self._map_standard_field(df.columns, 'oi')
            if oi_col:
                df_normalized['oi'] = pd.to_numeric(df[oi_col], errors='coerce')
            
            volume_col = self._map_standard_field(df.columns, 'volume')
            if volume_col:
                df_normalized['volume'] = pd.to_numeric(df[volume_col], errors='coerce')
        
        # 只保留标准列
        standard_cols = ['date', 'price']
        if data_type == 'futures':
            if 'oi' in df_normalized.columns:
                standard_cols.append('oi')
            if 'volume' in df_normalized.columns:
                standard_cols.append('volume')
        
        df_normalized = df_normalized[standard_cols].copy()
        
        # 去除重复日期
        df_normalized = df_normalized.drop_duplicates(subset=['date'], keep='first')
        
        # 按日期排序
        df_normalized = df_normalized.sort_values('date').reset_index(drop=True)
        
        # 向前填充缺失价格
        df_normalized['price'] = df_normalized['price'].ffill()
        
        return df_normalized
    
    def _extract_contract_code(self, filename: str, df: pd.DataFrame) -> str:
        """从文件名或数据中提取合约代码"""
        # 从文件名提取：如 cu2301.csv -> cu2301
        match = re.search(r'([a-z]+)(\d{4})', filename.lower())
        if match:
            return match.group(1) + match.group(2)
        
        # 从数据列中提取
        for col in df.columns:
            if '合约' in col or 'contract' in col.lower():
                first_value = df[col].iloc[0] if len(df) > 0 else None
                if first_value and isinstance(first_value, str):
                    return first_value
        
        return Path(filename).stem
    
    def _align_to_futures_trading_days(self):
        """将现货数据对齐到期货交易日"""
        if self.spot_data is None or len(self.futures_data) == 0:
            return
        
        # 收集所有期货合约的交易日期
        futures_dates = set()
        for contract_df in self.futures_data.values():
            if 'date' in contract_df.columns:
                futures_dates.update(contract_df['date'].dropna())
        
        if not futures_dates:
            return
        
        futures_dates = sorted(futures_dates)
        
        # 将现货价格对齐到期货交易日
        spot_prices = []
        spot_dates = []
        
        spot_date_price = dict(zip(
            pd.to_datetime(self.spot_data['date']), 
            self.spot_data['price']
        ))
        
        last_spot_price = None
        
        for fut_date in futures_dates:
            fut_date_dt = pd.to_datetime(fut_date)
            
            if fut_date_dt in spot_date_price:
                last_spot_price = spot_date_price[fut_date_dt]
                spot_prices.append(last_spot_price)
                spot_dates.append(fut_date_dt)
            else:
                if last_spot_price is not None:
                    spot_prices.append(last_spot_price)
                    spot_dates.append(fut_date_dt)
                else:
                    closest_price = None
                    min_diff = float('inf')
                    for spot_dt, spot_p in spot_date_price.items():
                        diff = abs((fut_date_dt - spot_dt).days)
                        if diff < min_diff:
                            min_diff = diff
                            closest_price = spot_p
                    spot_prices.append(closest_price)
                    spot_dates.append(fut_date_dt)
                    last_spot_price = closest_price
        
        self.spot_data = pd.DataFrame({
            'date': spot_dates,
            'price': spot_prices
        })
    
    def get_unified_panel(self) -> pd.DataFrame:
        """生成统一数据面板"""
        self._align_to_futures_trading_days()
        
        # 收集所有日期
        all_dates = set()
        if self.spot_data is not None:
            all_dates.update(self.spot_data['date'])
        for df in self.futures_data.values():
            all_dates.update(df['date'])
        
        if not all_dates:
            return pd.DataFrame()
        
        unified_dates = sorted(all_dates)
        panel = pd.DataFrame(index=unified_dates)
        panel.index.name = 'date'
        
        # 添加现货价格
        if self.spot_data is not None:
            spot_dict = dict(zip(self.spot_data['date'], self.spot_data['price']))
            panel['spot_price'] = [spot_dict.get(d, np.nan) for d in unified_dates]
        
        # 添加各期货合约价格和基差
        for contract_code, df in self.futures_data.items():
            fut_dict = dict(zip(df['date'], df['price']))
            panel[f'futures_{contract_code}'] = [fut_dict.get(d, np.nan) for d in unified_dates]
            
            # 计算基差
            if self.spot_data is not None:
                spot_dict = dict(zip(self.spot_data['date'], self.spot_data['price']))
                basis = [
                    spot_dict.get(d, np.nan) - fut_dict.get(d, np.nan)
                    for d in unified_dates
                ]
                panel[f'basis_{contract_code}'] = basis
        
        return panel
    
    def get_contract_info(self) -> Dict[str, Dict]:
        """获取合约信息"""
        info = {}
        
        for contract_code, df in self.futures_data.items():
            if df.empty or 'date' not in df.columns:
                continue
            
            dates = df['date'].dropna()
            if dates.empty:
                continue
            
            contract_info = {
                'start_date': dates.min(),
                'end_date': dates.max(),
                'trading_days': len(dates),
            }
            
            if 'oi' in df.columns:
                contract_info['avg_oi'] = df['oi'].mean()
                contract_info['max_oi'] = df['oi'].max()
            else:
                contract_info['avg_oi'] = None
            
            if 'volume' in df.columns:
                contract_info['avg_volume'] = df['volume'].mean()
            else:
                contract_info['avg_volume'] = None
            
            info[contract_code] = contract_info
        
        if self.spot_data is not None and not self.spot_data.empty:
            dates = self.spot_data['date'].dropna()
            if not dates.empty:
                info['spot'] = {
                    'start_date': dates.min(),
                    'end_date': dates.max(),
                    'trading_days': len(dates),
                    'avg_price': self.spot_data['price'].mean()
                }
        
        self.contract_info = info
        return info
    
    def data_quality_report(self) -> Dict:
        """生成数据质量报告"""
        report = {
            'scan_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'spot': {},
            'futures': {}
        }
        
        if self.spot_data is not None and not self.spot_data.empty:
            total_days = len(self.spot_data)
            valid_price_days = self.spot_data['price'].notna().sum()
            completeness = valid_price_days / total_days if total_days > 0 else 0
            
            report['spot'] = {
                'total_records': total_days,
                'valid_records': int(valid_price_days),
                'completeness': f"{completeness * 100:.2f}%",
                'date_range': f"{self.spot_data['date'].min()} 至 {self.spot_data['date'].max()}"
            }
        else:
            report['spot'] = {'status': '未找到现货数据'}
        
        if self.futures_data:
            report['futures']['contract_count'] = len(self.futures_data)
            report['futures']['contracts'] = {}
            
            for contract_code, df in self.futures_data.items():
                total_days = len(df)
                valid_price_days = df['price'].notna().sum() if 'price' in df.columns else 0
                completeness = valid_price_days / total_days if total_days > 0 else 0
                
                report['futures']['contracts'][contract_code] = {
                    'total_records': total_days,
                    'valid_records': int(valid_price_days),
                    'completeness': f"{completeness * 100:.2f}%",
                    'date_range': f"{df['date'].min()} 至 {df['date'].max()}" if 'date' in df.columns and not df.empty else "N/A"
                }
        else:
            report['futures'] = {'status': '未找到期货数据'}
        
        self.quality_report = report
        return report
    
    def print_quality_report(self):
        """打印数据质量报告"""
        report = self.data_quality_report()
        
        print("\n" + "="*60)
        print("数据质量报告")
        print("="*60)
        print(f"扫描时间: {report['scan_time']}\n")
        
        print("【现货数据】")
        if 'status' in report['spot']:
            print(f"  {report['spot']['status']}")
        else:
            print(f"  总记录数: {report['spot']['total_records']}")
            print(f"  有效记录数: {report['spot']['valid_records']}")
            print(f"  完整率: {report['spot']['completeness']}")
            print(f"  日期范围: {report['spot']['date_range']}")
        print()
        
        print("【期货数据】")
        if 'status' in report['futures']:
            print(f"  {report['futures']['status']}")
        else:
            print(f"  合约数量: {report['futures']['contract_count']}")
            for contract_code, contract_info in report['futures']['contracts'].items():
                print(f"\n  合约 {contract_code}:")
                print(f"    总记录数: {contract_info['total_records']}")
                print(f"    有效记录数: {contract_info['valid_records']}")
                print(f"    完整率: {contract_info['completeness']}")
                print(f"    日期范围: {contract_info['date_range']}")
        print()
        
        print("="*60)
