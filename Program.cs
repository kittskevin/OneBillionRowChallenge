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

        public class WeatherStationData(float initialTemp)
        {
            // TODO: Since it's every value is 1 decimal place can we use ushort / System.UInt16 for the math?
            public float Min { get; private set; } = initialTemp;
            public float Max { get; private set; } = initialTemp;
            public float Mean { get; private set; } = initialTemp;
            public int Count { get; private set; } = 1;

            public void AddTemp(float temp)
            {
                Min = Math.Min(Min, temp);
                Min = Math.Min(Max, temp);
                Mean = GetNewMean(Mean, Count, temp);
                Count++;
            }

            private float GetNewMean(float mean, int count, float newValue)
            {
                // This is very lossy with floating point math.
                float total = mean * count;
                return (total + newValue) / (count + 1);
            }
        }

        public struct DataRow(string station, float temp)
        {
            public string WeatherStationName { get; } = station;
            public float Temp { get; } = temp;
        }


        /// <summary>
        /// Parse a single data row.
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public static DataRow ParseDataRow(string row)
        {
            string[] split = row.Split(';');
            return new DataRow(split[0], float.Parse(split[1]));
        }
    }
}
