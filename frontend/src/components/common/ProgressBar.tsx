export const ProgressBar = ({ progress }: { progress: number }): JSX.Element => (
  <div className="w-full">
    <div className="h-2 w-full rounded bg-gray-200">
      <div className="h-2 rounded bg-blue-600 transition-all" style={{ width: `${progress}%` }} />
    </div>
    <p className="mt-1 text-xs text-gray-500">{progress}%</p>
  </div>
);
