"use client";

import { useMemo } from "react";
import {
  ScatterChart as RechartsScatterChart,
  Scatter,
  XAxis,
  YAxis,
  ZAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { useDarkMode } from "@/lib/use-dark-mode";
import {
  CATEGORICAL_LIGHT,
  CATEGORICAL_DARK,
  CHART_INK_LIGHT,
  CHART_INK_DARK,
  SINGLE_SERIES_LIGHT,
  SINGLE_SERIES_DARK,
} from "@/lib/chart-colors";

export interface TooltipField {
  key: string;
  label: string;
  format?: (value: unknown) => string;
}

interface ScatterChartProps<T extends Record<string, unknown>> {
  data: T[];
  title: string;
  xKey: keyof T & string;
  yKey: keyof T & string;
  xLabel: string;
  yLabel: string;
  colorKey?: keyof T & string;
  colorOrder?: string[]; // fixed category order — colors never depend on filtered rank
  sizeKey?: keyof T & string;
  tooltipFields: TooltipField[];
}

function CustomTooltip({
  active,
  payload,
  fields,
  ink,
  surface,
}: {
  active?: boolean;
  payload?: { payload: Record<string, unknown> }[];
  fields: TooltipField[];
  ink: typeof CHART_INK_LIGHT;
  surface: string;
}) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  return (
    <div
      style={{
        background: surface,
        border: `1px solid ${ink.grid}`,
        borderRadius: 6,
        padding: "8px 10px",
        fontSize: 12,
        color: ink.primary,
      }}
    >
      {fields.map((f) => (
        <div key={f.key} style={{ color: ink.secondary }}>
          <span style={{ color: ink.muted }}>{f.label}: </span>
          <span style={{ color: ink.primary }}>
            {f.format ? f.format(row[f.key]) : String(row[f.key] ?? "")}
          </span>
        </div>
      ))}
    </div>
  );
}

export function ScatterChart<T extends Record<string, unknown>>({
  data,
  title,
  xKey,
  yKey,
  xLabel,
  yLabel,
  colorKey,
  colorOrder,
  sizeKey,
  tooltipFields,
}: ScatterChartProps<T>) {
  const isDark = useDarkMode();
  const palette = isDark ? CATEGORICAL_DARK : CATEGORICAL_LIGHT;
  const ink = isDark ? CHART_INK_DARK : CHART_INK_LIGHT;
  const singleSeriesColor = isDark ? SINGLE_SERIES_DARK : SINGLE_SERIES_LIGHT;

  const groups = useMemo(() => {
    if (!colorKey) {
      return [{ name: "All", rows: data }];
    }
    const categories = colorOrder ?? Array.from(new Set(data.map((d) => String(d[colorKey]))));
    return categories
      .map((name) => ({
        name,
        rows: data.filter((d) => String(d[colorKey]) === name),
      }))
      .filter((g) => g.rows.length > 0);
  }, [data, colorKey, colorOrder]);

  const sizeDomain = useMemo(() => {
    if (!sizeKey) return [0, 1] as [number, number];
    const values = data.map((d) => Number(d[sizeKey]) || 0);
    return [Math.min(...values, 0), Math.max(...values, 1)] as [number, number];
  }, [data, sizeKey]);

  return (
    <div>
      <h3 style={{ color: ink.primary }} className="mb-2 text-sm font-semibold">
        {title}
      </h3>
      <ResponsiveContainer width="100%" height={360}>
        <RechartsScatterChart margin={{ top: 8, right: 16, bottom: 24, left: 8 }}>
          <CartesianGrid stroke={ink.grid} strokeDasharray="2 4" />
          <XAxis
            type="number"
            dataKey={xKey as string}
            name={xLabel}
            tick={{ fill: ink.muted, fontSize: 11 }}
            stroke={ink.grid}
            label={{ value: xLabel, position: "insideBottom", offset: -8, fill: ink.secondary, fontSize: 12 }}
          />
          <YAxis
            type="number"
            dataKey={yKey as string}
            name={yLabel}
            tick={{ fill: ink.muted, fontSize: 11 }}
            stroke={ink.grid}
            label={{ value: yLabel, angle: -90, position: "insideLeft", fill: ink.secondary, fontSize: 12 }}
          />
          {sizeKey && <ZAxis type="number" dataKey={sizeKey as string} range={[24, 320]} domain={sizeDomain} />}
          <Tooltip
            cursor={{ stroke: ink.muted, strokeDasharray: "2 4" }}
            content={<CustomTooltip fields={tooltipFields} ink={ink} surface={ink.surface} />}
          />
          {colorKey && (
            <Legend
              wrapperStyle={{ fontSize: 12 }}
              formatter={(value) => <span style={{ color: ink.secondary }}>{value}</span>}
            />
          )}
          {groups.map((group, i) => (
            <Scatter
              key={group.name}
              name={group.name}
              data={group.rows}
              fill={colorKey ? palette[i % palette.length] : singleSeriesColor}
              fillOpacity={0.75}
            />
          ))}
        </RechartsScatterChart>
      </ResponsiveContainer>
    </div>
  );
}
