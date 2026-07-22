import type { Player } from "@/lib/types";

export function TopValueTable({ players }: { players: Player[] }) {
  const top = [...players]
    .sort((a, b) => b.valueAdded - a.valueAdded)
    .slice(0, 10);

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-neutral-200 text-left text-xs text-neutral-500 dark:border-neutral-800 dark:text-neutral-400">
            <th className="py-1.5 pr-3">Player</th>
            <th className="py-1.5 pr-3">Team</th>
            <th className="py-1.5 pr-3">Pos</th>
            <th className="py-1.5 pr-3 text-right">Price</th>
            <th className="py-1.5 pr-3 text-right">Value Added</th>
            <th className="py-1.5 pr-3 text-right">Pts / £m</th>
            <th className="py-1.5 text-right">Total Points</th>
          </tr>
        </thead>
        <tbody>
          {top.map((p) => (
            <tr
              key={p.id}
              className="border-b border-neutral-100 text-neutral-800 dark:border-neutral-900 dark:text-neutral-200"
            >
              <td className="py-1.5 pr-3">{p.webName}</td>
              <td className="py-1.5 pr-3 text-neutral-500 dark:text-neutral-400">{p.teamName}</td>
              <td className="py-1.5 pr-3 text-neutral-500 dark:text-neutral-400">{p.position}</td>
              <td className="py-1.5 pr-3 text-right tabular-nums">£{p.price.toFixed(1)}m</td>
              <td className="py-1.5 pr-3 text-right tabular-nums">{p.valueAdded.toFixed(2)}</td>
              <td className="py-1.5 pr-3 text-right tabular-nums">{p.pointsPerMillion.toFixed(1)}</td>
              <td className="py-1.5 text-right tabular-nums">{p.totalPoints}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
