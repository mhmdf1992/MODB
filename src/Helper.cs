using System;

namespace MO.MODB{
    public class Helper{
        public static long ConvertToLong(char[] str){
            int i=0;
            long sum=0;
            while(i < str.Length && str[i]!='\0')
            {
                if(str[i]< 48 || str[i] > 57)
                    throw new Exception($"Unable to convert it into integer ({str[i]})");
                else
                {
                    sum = sum*10 + (str[i] - 48);
                    i++;
                }
            }
            return sum;
        }

        public static int ConvertToInt(char[] str){
            int i=0,sum=0;
            while(i < str.Length && str[i]!='\0')
            {
                if(str[i]< 48 || str[i] > 57)
                    throw new Exception($"Unable to convert it into integer ({str[i]})");
                else
                {
                    sum = sum*10 + (str[i] - 48);
                    i++;
                }
            }
            return sum;
        }
    }
}