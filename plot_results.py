#!/usr/bin/env python3
"""
Построение графиков по результатам бенчмарка Data Skew Analysis.

Использование:
    python3 plot_results.py [csv_path] [output_dir]

По умолчанию читает /tmp/benchmark_results.csv,
сохраняет графики в /tmp/.
"""

import sys
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

CSV_PATH   = sys.argv[1] if len(sys.argv) > 1 else '/tmp/benchmark_results.csv'
OUTPUT_DIR = sys.argv[2] if len(sys.argv) > 2 else '/tmp'

SKEW_LEVELS = ['WEAK', 'MEDIUM', 'STRONG']
SKEW_LABELS = {'WEAK': 'Слабый (20%)', 'MEDIUM': 'Средний (50%)', 'STRONG': 'Сильный (80%)'}

PALETTE = [
    '#4e79a7', '#f28e2b', '#e15759', '#76b7b2',
    '#59a14f', '#edc948', '#b07aa1', '#ff9da7',
]


def load(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df['skew_level'] = pd.Categorical(df['skew_level'], categories=SKEW_LEVELS, ordered=True)
    return df.sort_values(['skew_level', 'method'])


def grouped_bar(df: pd.DataFrame, metric: str, ylabel: str, title: str, fname: str,
                transform=None):
    methods = df['method'].unique().tolist()
    x = np.arange(len(SKEW_LEVELS))
    width = 0.8 / len(methods)

    fig, ax = plt.subplots(figsize=(14, 7))
    for i, method in enumerate(methods):
        vals = []
        for sl in SKEW_LEVELS:
            row = df[(df['method'] == method) & (df['skew_level'] == sl)]
            v = float(row[metric].iloc[0]) if len(row) > 0 else 0.0
            vals.append(transform(v) if transform else v)
        offset = (i - len(methods) / 2 + 0.5) * width
        bars = ax.bar(x + offset, vals, width * 0.92, label=method, color=PALETTE[i % len(PALETTE)])

    ax.set_xlabel('Уровень перекоса', fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(title, fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels([SKEW_LABELS[sl] for sl in SKEW_LEVELS])
    ax.legend(loc='upper left', fontsize=9, ncol=2)
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    out = os.path.join(OUTPUT_DIR, fname)
    plt.savefig(out, dpi=150)
    plt.close()
    print(f'Сохранён: {out}')


def line_chart(df: pd.DataFrame, metric: str, ylabel: str, title: str, fname: str,
               transform=None):
    methods = df['method'].unique().tolist()
    x = np.arange(len(SKEW_LEVELS))

    fig, ax = plt.subplots(figsize=(12, 6))
    for i, method in enumerate(methods):
        vals = []
        for sl in SKEW_LEVELS:
            row = df[(df['method'] == method) & (df['skew_level'] == sl)]
            v = float(row[metric].iloc[0]) if len(row) > 0 else 0.0
            vals.append(transform(v) if transform else v)
        ax.plot(x, vals, marker='o', label=method, color=PALETTE[i % len(PALETTE)], linewidth=2)

    ax.set_xlabel('Уровень перекоса', fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(title, fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels([SKEW_LABELS[sl] for sl in SKEW_LEVELS])
    ax.legend(loc='upper left', fontsize=9, ncol=2)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    out = os.path.join(OUTPUT_DIR, fname)
    plt.savefig(out, dpi=150)
    plt.close()
    print(f'Сохранён: {out}')


def mb(b: float) -> float:
    return b / (1024 ** 2)


def main():
    if not os.path.exists(CSV_PATH):
        print(f'Файл не найден: {CSV_PATH}')
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = load(CSV_PATH)

    print(f'Загружено {len(df)} строк из {CSV_PATH}')
    print(f'Методы: {df["method"].unique().tolist()}')
    print(f'Уровни перекоса: {df["skew_level"].unique().tolist()}')
    print()

    # 1. Время выполнения — bar chart
    grouped_bar(df, 'avg_time_ms', 'Среднее время (мс)',
                'Время выполнения join по методам и уровням перекоса',
                'execution_time_bar.png')

    # 2. Время выполнения — line chart (удобно сравнивать тренды)
    line_chart(df, 'avg_time_ms', 'Среднее время (мс)',
               'Зависимость времени выполнения от уровня перекоса',
               'execution_time_line.png')

    # 3. Shuffle Read (MB)
    grouped_bar(df, 'avg_shuffle_read_bytes', 'Shuffle Read (МБ)',
                'Объём Shuffle Read по методам',
                'shuffle_read.png', transform=mb)

    # 4. Shuffle Write (MB)
    grouped_bar(df, 'avg_shuffle_write_bytes', 'Shuffle Write (МБ)',
                'Объём Shuffle Write по методам',
                'shuffle_write.png', transform=mb)

    # 5. Peak Memory (MB)
    grouped_bar(df, 'peak_memory_bytes', 'Пиковая память (МБ)',
                'Максимальное использование памяти исполнителями',
                'peak_memory.png', transform=mb)

    # 6. Время самой медленной задачи
    grouped_bar(df, 'avg_max_task_ms', 'Время самой медленной задачи (мс)',
                'Время самой медленной задачи (индикатор перекоса)',
                'slowest_task.png')

    # 7. Сводный график — 4 метрики, только STRONG skew
    strong = df[df['skew_level'] == 'STRONG'].copy()
    if not strong.empty:
        fig, axes = plt.subplots(2, 2, figsize=(16, 10))
        fig.suptitle('Сравнение методов при сильном перекосе (80%)', fontsize=15)

        metrics = [
            ('avg_time_ms',           'Время (мс)',            axes[0, 0]),
            ('avg_shuffle_read_bytes', 'Shuffle Read (МБ)',     axes[0, 1]),
            ('avg_shuffle_write_bytes','Shuffle Write (МБ)',    axes[1, 0]),
            ('avg_max_task_ms',        'Макс. задача (мс)',     axes[1, 1]),
        ]
        for col, label, ax in metrics:
            tfm = mb if 'bytes' in col else None
            vals = [tfm(float(strong[strong['method'] == m][col].iloc[0]))
                    if tfm else float(strong[strong['method'] == m][col].iloc[0])
                    for m in strong['method'].unique()]
            colors = [PALETTE[i % len(PALETTE)] for i in range(len(strong['method'].unique()))]
            bars = ax.bar(strong['method'].unique(), vals, color=colors)
            ax.set_title(label)
            ax.set_ylabel(label)
            ax.tick_params(axis='x', rotation=30)
            ax.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        out = os.path.join(OUTPUT_DIR, 'summary_strong_skew.png')
        plt.savefig(out, dpi=150)
        plt.close()
        print(f'Сохранён: {out}')

    print(f'\nВсе графики сохранены в: {OUTPUT_DIR}')


if __name__ == '__main__':
    main()
