const maxRows10FrameClassName =
  "max-w-full overflow-x-auto overflow-y-auto [scrollbar-gutter:stable] max-h-[35.25rem]";
const uncappedFrameClassName = "max-w-full overflow-x-auto overflow-y-hidden";

export const stickyResultsTableHeaderClassName = "sticky top-0 z-10";
export const signalsResultsScrollFrameClassName =
  "max-w-full overflow-x-hidden overflow-y-auto [scrollbar-gutter:stable] max-h-[35.25rem]";
export const mobileResultsScrollFrameClassName =
  "max-w-full overflow-x-hidden overflow-y-auto [scrollbar-gutter:stable] max-h-[42rem]";

export function resultsTableFrameClassName(rowCount: number, options: { always?: boolean } = {}): string {
  return options.always || rowCount > 10 ? maxRows10FrameClassName : uncappedFrameClassName;
}
