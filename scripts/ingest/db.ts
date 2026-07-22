import { config } from "dotenv";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import * as schema from "@/lib/db/schema";

config({ path: ".env.local" });

const connectionString = process.env.SUPABASE_DB_URL;
if (!connectionString) {
  throw new Error("SUPABASE_DB_URL is not set");
}

const client = postgres(connectionString, { prepare: false, ssl: "require" });

export const db = drizzle(client, { schema });

export async function closeDb(): Promise<void> {
  await client.end();
}
