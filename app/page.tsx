import { getAllPlayers, getLastRefresh } from "@/lib/db/queries";
import { DashboardClient } from "@/components/dashboard-client";

export const revalidate = 3600;

export default async function Home() {
  const [players, lastUpdated] = await Promise.all([
    getAllPlayers(),
    getLastRefresh(),
  ]);

  return <DashboardClient players={players} lastUpdated={lastUpdated} />;
}
