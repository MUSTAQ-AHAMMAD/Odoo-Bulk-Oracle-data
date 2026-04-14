import { useForm } from 'react-hook-form';
import { yupResolver } from '@hookform/resolvers/yup';
import * as yup from 'yup';
import { useNavigate, Link } from 'react-router-dom';
import client from '../api/client';
import { useAuth } from '../context/AuthContext';

type FormValues = { email: string; password: string };

const schema = yup.object({ email: yup.string().email().required(), password: yup.string().min(8).required() });

export default function Login(): JSX.Element {
  const navigate = useNavigate();
  const { setToken } = useAuth();
  const { register, handleSubmit, formState: { errors } } = useForm<FormValues>({ resolver: yupResolver(schema) });

  const onSubmit = async (values: FormValues): Promise<void> => {
    const response = await client.post('/api/auth/login', values);
    setToken(response.data.token);
    navigate('/dashboard');
  };

  return (
    <div className="mx-auto mt-20 max-w-md space-y-4 rounded border p-6">
      <h2 className="text-xl font-bold">Login</h2>
      <form className="space-y-3" onSubmit={handleSubmit(onSubmit)}>
        <input {...register('email')} placeholder="Email" className="w-full rounded border p-2" />
        {errors.email && <p className="text-sm text-red-600">{errors.email.message}</p>}
        <input type="password" {...register('password')} placeholder="Password" className="w-full rounded border p-2" />
        {errors.password && <p className="text-sm text-red-600">{errors.password.message}</p>}
        <button className="w-full rounded bg-blue-600 p-2 text-white" type="submit">Login</button>
      </form>
      <Link className="text-sm text-blue-600" to="/register">Create account</Link>
    </div>
  );
}
