// Copyright (c) Damien Guard.  All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Originally published at http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net

using System;

namespace SimpleKafka.Common
{
    /// <summary>
    /// This code was originally from the copyrighted code listed above but was modified significantly
    /// as the original code was not thread safe and did not match was was required of this driver. This
    /// class now provides a static lib which will do the simple CRC calculation required by Kafka servers.
    /// </summary>
    public static class Crc32Provider
    {
        public const UInt32 DefaultPolynomial = 0xedb88320u;
        public const UInt32 DefaultSeed = 0xffffffffu;
        private static readonly UInt32[] PolynomialTable;

        static Crc32Provider()
        {
            PolynomialTable = InitializeTable(DefaultPolynomial);
        }

        public static UInt32 Compute(byte[] buffer)
        {
            return ~CalculateHash(buffer, 0, buffer.Length);
        }

        public static UInt32 Compute(byte[] buffer, int offset, int length)
        {
            return ~CalculateHash(buffer, offset, length);
        }

        private static UInt32[] InitializeTable(UInt32 polynomial)
        {
            var createTable = new UInt32[256];
            for (var i = 0; i < 256; i++)
            {
                var entry = (UInt32)i;
                for (var j = 0; j < 8; j++)
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ polynomial;
                    else
                        entry = entry >> 1;
                createTable[i] = entry;
            }

            return createTable;
        }

        private static UInt32 CalculateHash(byte[] buffer, int offset, int length)
        {
            var crc = DefaultSeed;
            while (length-- > 0)
            {
                crc = (crc >> 8) ^ PolynomialTable[buffer[offset++] ^ crc & 0xff];
            }
            return crc;
        }

    }
}