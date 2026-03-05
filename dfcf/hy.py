import requests
import json
from typing import Dict, List, Any


def fetch_industry_data() -> Dict[str, Any]:
    """
    从东方财富API获取行业资金流向数据
    """
    url = "https://push2.eastmoney.com/api/qt/clist/get?fid=f62&po=1&pz=50&pn=1&np=1&fltt=2&invt=2&ut=8dec03ba335b81bf4ebdf7b29ec27d15&fs=m:90+t:2&fields=f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124,f1,f13"

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"网络请求错误: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {e}")
        return None


def analyze_industry_data(data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    分析行业数据，返回多种排序结果
    """
    if not data or 'data' not in data or 'diff' not in data['data']:
        print("数据格式错误或数据为空")
        return {}

    industries = data['data']['diff']

    # 按净流入资金排序（f62）
    sorted_by_net_inflow = sorted(
        industries,
        key=lambda x: x.get('f62', 0),
        reverse=True
    )[:5]

    # 按涨跌幅排序（f3）
    sorted_by_change = sorted(
        industries,
        key=lambda x: x.get('f3', 0),
        reverse=True
    )[:5]

    # 按主力净流入排序（f66）
    sorted_by_main_inflow = sorted(
        industries,
        key=lambda x: x.get('f66', 0),
        reverse=True
    )[:5]

    return {
        'net_inflow': sorted_by_net_inflow,
        'price_change': sorted_by_change,
        'main_inflow': sorted_by_main_inflow
    }


def format_industry_info(industry: Dict[str, Any]) -> Dict[str, Any]:
    """
    格式化行业信息
    """
    return {
        '行业名称': industry.get('f14', '未知'),
        '行业代码': industry.get('f12', ''),
        '净流入资金(亿元)': round(industry.get('f62', 0) / 100000000, 2),
        '涨跌幅(%)': industry.get('f3', 0),
        '主力净流入(亿元)': round(industry.get('f66', 0) / 100000000, 2),
        '超大单净流入(亿元)': round(industry.get('f69', 0) / 100000000, 2),
        '大单净流入(亿元)': round(industry.get('f72', 0) / 100000000, 2),
        '中单净流入(亿元)': round(industry.get('f78', 0) / 100000000, 2),
        '小单净流入(亿元)': round(industry.get('f84', 0) / 100000000, 2),
        '成交额(万元)': round(industry.get('f2', 0), 2),
        '相关股票': f"{industry.get('f204', '')}({industry.get('f205', '')})"
    }


def display_results(results: Dict[str, List[Dict[str, Any]]]):
    """
    显示分析结果
    """
    print("=" * 80)
    print("行业资金流向分析报告")
    print("=" * 80)

    # 1. 按净流入资金排序
    print("\n📊 按净流入资金排序的前五行业:")
    print("-" * 80)
    for i, industry in enumerate(results.get('net_inflow', []), 1):
        info = format_industry_info(industry)
        print(f"{i}. {info['行业名称']}")
        print(f"   净流入资金: {info['净流入资金(亿元)']}亿元 | 涨跌幅: {info['涨跌幅(%)']}%")
        print(f"   主力净流入: {info['主力净流入(亿元)']}亿元 | 相关股票: {info['相关股票']}")
        print("-" * 50)

    # 2. 按涨跌幅排序
    print("\n📈 按涨跌幅排序的前五行业:")
    print("-" * 80)
    for i, industry in enumerate(results.get('price_change', []), 1):
        info = format_industry_info(industry)
        print(f"{i}. {info['行业名称']}")
        print(f"   涨跌幅: {info['涨跌幅(%)']}% | 净流入资金: {info['净流入资金(亿元)']}亿元")
        print(f"   成交额: {info['成交额(万元)']}万元 | 相关股票: {info['相关股票']}")
        print("-" * 50)

    # 3. 数据统计摘要
    print("\n📋 数据统计摘要:")
    print("-" * 80)
    total_net_inflow = sum(industry.get('f62', 0) for industry in results.get('net_inflow', []))
    avg_net_inflow = total_net_inflow / len(results.get('net_inflow', [])) if results.get('net_inflow') else 0

    total_change = sum(industry.get('f3', 0) for industry in results.get('price_change', []))
    avg_change = total_change / len(results.get('price_change', [])) if results.get('price_change') else 0

    print(f"前五行业总净流入: {round(total_net_inflow / 100000000, 2)}亿元")
    print(f"前五行业平均净流入: {round(avg_net_inflow / 100000000, 2)}亿元")
    print(f"前五行业平均涨跌幅: {round(avg_change, 2)}%")

    # 4. 修正后的资金流向分布分析
    print("\n💹 资金流向分布分析（按净流入资金排序前五行业）:")
    print("-" * 80)

    for i, industry in enumerate(results.get('net_inflow', []), 1):
        info = format_industry_info(industry)

        # 计算各类型资金占比
        net_inflow = info['净流入资金(亿元)']

        # 检查净流入资金是否为0，避免除零错误
        if net_inflow == 0:
            ratios = {
                '超大单': 0,
                '大单': 0,
                '中单': 0,
                '小单': 0
            }
        else:
            ratios = {
                '超大单': round(info['超大单净流入(亿元)'] / net_inflow * 100, 1),
                '大单': round(info['大单净流入(亿元)'] / net_inflow * 100, 1),
                '中单': round(info['中单净流入(亿元)'] / net_inflow * 100, 1),
                '小单': round(info['小单净流入(亿元)'] / net_inflow * 100, 1)
            }

        # 打印资金流向分布
        print(f"{i}. {info['行业名称']}")
        print(f"   总净流入: {net_inflow}亿元")

        # 打印各类型资金流入情况
        fund_types = [
            ('超大单', info['超大单净流入(亿元)'], ratios['超大单']),
            ('大单', info['大单净流入(亿元)'], ratios['大单']),
            ('中单', info['中单净流入(亿元)'], ratios['中单']),
            ('小单', info['小单净流入(亿元)'], ratios['小单'])
        ]

        for fund_type, amount, ratio in fund_types:
            # 使用不同符号表示资金流入流出
            sign = "↑" if amount >= 0 else "↓"
            color_start = "\033[92m" if amount >= 0 else "\033[91m"  # 绿色表示流入，红色表示流出
            color_end = "\033[0m"

            print(f"   {fund_type}: {color_start}{sign}{abs(amount)}亿元 ({ratio}%){color_end}")

        print("-" * 50)

    # 5. 主力资金流向分析
    print("\n💰 主力资金流向分析（按主力净流入排序前五行业）:")
    print("-" * 80)
    for i, industry in enumerate(results.get('main_inflow', []), 1):
        info = format_industry_info(industry)
        main_inflow = info['主力净流入(亿元)']

        # 使用不同符号表示主力资金流入流出
        sign = "↑" if main_inflow >= 0 else "↓"
        color_start = "\033[92m" if main_inflow >= 0 else "\033[91m"
        color_end = "\033[0m"

        print(f"{i}. {info['行业名称']}")
        print(f"   主力净流入: {color_start}{sign}{abs(main_inflow)}亿元{color_end}")
        print(f"   净流入资金: {info['净流入资金(亿元)']}亿元 | 涨跌幅: {info['涨跌幅(%)']}%")
        print("-" * 50)


def save_to_json(data: Dict[str, Any], filename: str = "industry_analysis.json"):
    """
    将分析结果保存为JSON文件
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\n💾 分析结果已保存到: {filename}")
    except Exception as e:
        print(f"保存文件时出错: {e}")


def calculate_fund_distribution(industries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    计算资金流向分布统计
    """
    if not industries:
        return {}

    total_net_inflow = sum(industry.get('f62', 0) for industry in industries)
    total_super_large = sum(industry.get('f69', 0) for industry in industries)
    total_large = sum(industry.get('f72', 0) for industry in industries)
    total_medium = sum(industry.get('f78', 0) for industry in industries)
    total_small = sum(industry.get('f84', 0) for industry in industries)

    # 转换为亿元
    total_net_inflow_yi = total_net_inflow / 100000000

    # 计算各类型资金占比
    if total_net_inflow != 0:
        super_large_ratio = round(total_super_large / total_net_inflow * 100, 1)
        large_ratio = round(total_large / total_net_inflow * 100, 1)
        medium_ratio = round(total_medium / total_net_inflow * 100, 1)
        small_ratio = round(total_small / total_net_inflow * 100, 1)
    else:
        super_large_ratio = large_ratio = medium_ratio = small_ratio = 0

    return {
        '总净流入(亿元)': round(total_net_inflow_yi, 2),
        '超大单净流入(亿元)': round(total_super_large / 100000000, 2),
        '大单净流入(亿元)': round(total_large / 100000000, 2),
        '中单净流入(亿元)': round(total_medium / 100000000, 2),
        '小单净流入(亿元)': round(total_small / 100000000, 2),
        '超大单占比(%)': super_large_ratio,
        '大单占比(%)': large_ratio,
        '中单占比(%)': medium_ratio,
        '小单占比(%)': small_ratio
    }


def main():
    """
    主函数
    """
    print("开始获取行业资金流向数据...")

    # 获取数据
    raw_data = fetch_industry_data()
    if not raw_data:
        print("无法获取数据，程序退出。")
        return

    # 分析数据
    print("数据获取成功，正在分析...")
    results = analyze_industry_data(raw_data)

    if not results:
        print("数据分析失败，程序退出。")
        return

    # 显示结果
    display_results(results)

    # 计算并显示资金分布统计
    print("\n📊 资金分布总体统计（按净流入资金排序前五行业）:")
    print("-" * 80)
    distribution_stats = calculate_fund_distribution(results.get('net_inflow', []))

    for key, value in distribution_stats.items():
        if '占比' in key:
            print(f"{key}: {value}%")
        else:
            print(f"{key}: {value}")

    # 保存完整结果
    formatted_results = {
        '按净流入资金排序': [format_industry_info(industry) for industry in results.get('net_inflow', [])],
        '按涨跌幅排序': [format_industry_info(industry) for industry in results.get('price_change', [])],
        '按主力净流入排序': [format_industry_info(industry) for industry in results.get('main_inflow', [])],
        '资金分布统计': distribution_stats,
        '原始数据统计': {
            '行业总数': raw_data['data']['total'],
            '分析时间': raw_data.get('svr', '未知')
        }
    }

    save_to_json(formatted_results)

    print("\n" + "=" * 80)
    print("分析完成！")


if __name__ == "__main__":
    # 检查依赖库
    try:
        import requests
    except ImportError:
        print("缺少依赖库，请先安装requests库:")
        print("pip install requests")
        exit(1)

    main()