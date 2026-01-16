# -*- coding: utf-8 -*-
"""
数据预处理脚本
一次性处理所有CSV文件，生成统一格式的数据文件
运行一次后，后续分析可以直接使用预处理好的数据
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

# 导入中文文件名的模块
def import_module_by_name(module_name, file_path):
    """动态导入模块（支持中文文件名）"""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# 导入数据网关模块
current_dir = os.path.dirname(os.path.abspath(__file__))
data_gateway_module = import_module_by_name('数据网关', os.path.join(current_dir, '数据网关.py'))
OmniDataGateway = data_gateway_module.OmniDataGateway


def preprocess_and_save(data_dir=".", output_file="processed_data.pkl"):
    """
    预处理数据并保存
    
    Args:
        data_dir: 数据目录
        output_file: 输出文件名
    """
    print("="*70)
    print("数据预处理")
    print("="*70)
    
    # 加载数据
    print("\n【步骤1】扫描并加载CSV文件...")
    gateway = OmniDataGateway(data_dir=data_dir)
    stats = gateway.scan_and_load(recursive=True)
    
    print(f"\n数据加载完成:")
    print(f"  现货文件: {stats['spot_count']} 个")
    print(f"  期货文件: {stats['futures_count']} 个")
    
    if stats['spot_count'] == 0 and stats['futures_count'] == 0:
        print("\n❌ 错误: 未找到任何数据文件")
        return False
    
    # 生成统一数据面板
    print("\n【步骤2】生成统一数据面板...")
    panel = gateway.get_unified_panel()
    
    if panel.empty:
        print("\n❌ 错误: 数据面板为空")
        return False
    
    print(f"  数据面板形状: {panel.shape}")
    print(f"  日期范围: {panel.index.min()} 至 {panel.index.max()}")
    
    # 获取合约信息
    print("\n【步骤3】获取合约信息...")
    contract_info = gateway.get_contract_info()
    
    # 保存处理好的数据
    print(f"\n【步骤4】保存预处理数据到: {output_file}")
    
    processed_data = {
        'panel': panel,
        'contract_info': contract_info,
        'quality_report': gateway.data_quality_report(),
        'stats': stats,
        'spot_data': gateway.spot_data,
        'futures_data': gateway.futures_data
    }
    
    with open(output_file, 'wb') as f:
        pickle.dump(processed_data, f)
    
    # 同时保存为CSV（便于查看）
    csv_file = output_file.replace('.pkl', '_panel.csv')
    panel.to_csv(csv_file, encoding='utf-8-sig')
    print(f"  统一数据面板CSV: {csv_file}")
    
    print("\n" + "="*70)
    print("✓ 数据预处理完成！")
    print("="*70)
    print(f"\n预处理数据已保存到: {output_file}")
    print("后续分析可以直接使用预处理好的数据，无需重新扫描CSV文件")
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='预处理CSV数据')
    parser.add_argument('--dir', type=str, default='.', help='数据目录（默认：当前目录）')
    parser.add_argument('--output', type=str, default='processed_data.pkl', help='输出文件名（默认：processed_data.pkl）')
    
    args = parser.parse_args()
    
    success = preprocess_and_save(data_dir=args.dir, output_file=args.output)
    
    if not success:
        sys.exit(1)
