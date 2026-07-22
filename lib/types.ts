export type { Player } from "@/lib/db/schema";

export const POSITIONS = ["All", "GK", "DEF", "MID", "FWD"] as const;
export type Position = (typeof POSITIONS)[number];
