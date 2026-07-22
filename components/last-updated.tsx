export function LastUpdated({ date }: { date: Date | null }) {
  if (!date) {
    return (
      <p className="text-xs text-neutral-500 dark:text-neutral-400">
        Data not refreshed yet
      </p>
    );
  }
  return (
    <p className="text-xs text-neutral-500 dark:text-neutral-400">
      Data last updated:{" "}
      {date.toLocaleString("en-GB", {
        dateStyle: "medium",
        timeStyle: "short",
        timeZone: "UTC",
      })}{" "}
      UTC
    </p>
  );
}
