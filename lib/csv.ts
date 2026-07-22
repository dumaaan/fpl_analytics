export function toCsv<T extends Record<string, unknown>>(
  rows: T[],
  columns: (keyof T)[],
): string {
  const header = columns.join(",");
  const body = rows
    .map((row) =>
      columns
        .map((col) => {
          const value = row[col];
          const cell = value === null || value === undefined ? "" : String(value);
          return cell.includes(",") || cell.includes('"')
            ? `"${cell.replace(/"/g, '""')}"`
            : cell;
        })
        .join(","),
    )
    .join("\n");
  return `${header}\n${body}`;
}

export function downloadCsv(filename: string, csv: string): void {
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}
