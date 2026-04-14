import { useDropzone } from 'react-dropzone';

interface DropZoneProps {
  onFile: (file: File) => void;
}

export const DropZone = ({ onFile }: DropZoneProps): JSX.Element => {
  const { getRootProps, getInputProps, acceptedFiles } = useDropzone({
    multiple: false,
    maxSize: 100 * 1024 * 1024,
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.ms-excel': ['.xls'],
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx']
    },
    onDropAccepted: (files) => {
      const file = files[0];
      if (file) onFile(file);
    }
  });

  return (
    <div {...getRootProps()} className="cursor-pointer rounded border-2 border-dashed p-10 text-center">
      <input {...getInputProps()} />
      <p>Drag and drop CSV/Excel file, or click to select</p>
      {acceptedFiles[0] && <p className="mt-2 text-sm">{acceptedFiles[0].name}</p>}
    </div>
  );
};
