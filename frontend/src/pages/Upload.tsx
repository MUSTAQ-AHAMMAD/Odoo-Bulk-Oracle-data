import { useNavigate } from 'react-router-dom';
import client from '../api/client';
import { DropZone } from '../components/upload/DropZone';

export default function Upload(): JSX.Element {
  const navigate = useNavigate();

  const handleFile = async (file: File): Promise<void> => {
    const form = new FormData();
    form.append('file', file);
    const response = await client.post('/api/upload', form, {
      headers: { 'Content-Type': 'multipart/form-data' }
    });
    navigate(`/preview/${response.data.jobId}`);
  };

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">Upload File</h2>
      <DropZone onFile={handleFile} />
    </div>
  );
}
