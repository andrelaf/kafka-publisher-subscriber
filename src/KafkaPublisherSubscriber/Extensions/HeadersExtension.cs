using Confluent.Kafka;
using System;
using System.Text;

namespace KafkaPublisherSubscriber.Extensions
{
    public static class HeadersExtension
    {
        public static T GetHeaderAs<T>(this Headers headers, string headerKey)
        {
            if (headers != null && headers.TryGetLastBytes(headerKey, out byte[] headerBytes))
            {
                string headerString = Encoding.UTF8.GetString(headerBytes);

                try
                {
                    return (T)Convert.ChangeType(headerString, typeof(T));
                }
                catch
                {
                    // Conversão falhou, retorna o valor padrão para o tipo T
                    return default(T);
                }
            }

            // Cabeçalho não encontrado, retorna o valor padrão para o tipo T
            return default(T);
        }
    }
}