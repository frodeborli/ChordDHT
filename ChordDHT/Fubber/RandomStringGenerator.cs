using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fubber
{
    public class RandomStringGenerator
    {
        private const string Characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        private static readonly Random random = new Random();

        public static string GenerateRandomString(int maxNumberOfCharacters)
        {
            if (maxNumberOfCharacters <= 0)
            {
                throw new ArgumentException("maxNumberOfCharacters must be greater than zero.");
            }

            StringBuilder sb = new StringBuilder();
            int charactersLength = Characters.Length;

            for (int i = 0; i < maxNumberOfCharacters; i++)
            {
                char randomChar = Characters[random.Next(charactersLength)];
                sb.Append(randomChar);
            }

            return sb.ToString();
        }
    }
}
