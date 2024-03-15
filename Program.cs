using System;
using System.Diagnostics;

namespace obrc
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            string inputCsv = Path.Combine(Environment.CurrentDirectory, @"data\weather_stations.csv");
            await SequentialRead(inputCsv);
        }

        /// <summary>
        /// Sequential streaming read.
        /// Probably slowest reasonable solution allowing arbitrary size file (does not try to load the entire file into memory).
        /// </summary>
        /// <returns></returns>
        public static async Task SequentialRead(string inputFile)
        {
            // TODO: Is it faster to use a hashtable and then sort at the end or using a tree / sorted dictionary?
            // Likely fastest to keep a sorted list of the weather station names and then output them by looking them up in the hash table at the end in order.
            IDictionary<string, WeatherStationData> weatherData = new SortedDictionary<string, WeatherStationData>(StringComparer.OrdinalIgnoreCase);

            // TODO: This can be improved by creating our own stream reader that reads out DataRow struct instead of line by line and then parsing the lines.
            // TODO: Test different values for the buffer size.
            //
            using (StreamReader fileStreamReader = new StreamReader(new BufferedStream(new FileStream(inputFile, FileMode.Open, FileAccess.Read), bufferSize: 4096)))
            {
                // Read row by row until we reach the end of the stream.
                while (!fileStreamReader.EndOfStream)
                {
                    string? row = await fileStreamReader.ReadLineAsync();
                    if (row != null && !row.StartsWith("#"))
                    {
                        DataRow data = ParseDataRow(row);

                        if (!weatherData.ContainsKey(data.WeatherStationName))
                        {
                            weatherData.Add(data.WeatherStationName, new WeatherStationData(data.Temp));
                        }
                        else
                        {
                            weatherData[data.WeatherStationName].AddTemp(data.Temp);
                        }
                    }
                }
            }

            Console.Write("{");
            foreach (KeyValuePair<string, WeatherStationData> kvp in weatherData)
            {
                // TODO: Fix formatting at the end.
                Console.Write($"{kvp.Key}={kvp.Value.Min}/{kvp.Value.Mean}/{kvp.Value.Max}, ");
            }
            Console.WriteLine("}");
        }

        public class WeatherStationData(int initialTemp)
        {
            // Internal representation of the temps are the temp * 10_000.
            // 
            private const int InternalRepresentationFactor = 10_000;

            private int _min = initialTemp;

            private int _max = initialTemp;

            private long _total = initialTemp;

            private int _count = 1;

            public string Min => IntToFloatString(_min);
            public string Max => IntToFloatString(_max);

            public string Mean => $"{(_total * 1.0) / _count / InternalRepresentationFactor:0.#}";

            public void AddTemp(int temp)
            {
                _min = Math.Min(_min, temp);
                _max = Math.Max(_max, temp);
                _total += temp;
                _count++;
            }

            private string IntToFloatString(int x)
            {
                return $"{x / InternalRepresentationFactor}.{x % InternalRepresentationFactor}";
            }
        }

        public struct DataRow(string station, int temp)
        {
            public string WeatherStationName { get; } = station;
            public int Temp { get; } = temp;
        }


        /// <summary>
        /// Parse a single data row.
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public static DataRow ParseDataRow(string row)
        {
            // Parse the temp without the decimal and the internal representation will be x10_000
            string[] split = row.Split(';');
            return new DataRow(split[0], int.Parse(split[1].Replace(".", ""))); 
        }
    }
}
