#include <stdio.h>
#include <stdint.h>
#include <string.h>

int main() {
    //unsigned long long large_value = 281474976710655LL;  // An 8-byte long long value
    unsigned long long large_value = 256LL;  // An 8-byte long long value
    unsigned char byte_array8[8];
    unsigned char byte_array6[6];  // Character array of 6 bytes
    unsigned char byte_array_mc[6];
    
    

    // Copy the 6 most significant bytes of the long long value into the character array
    for (int i = 0; i < 6; ++i) {
        byte_array6[i] = (large_value >> (i * 8)) & 0xFF;
        
    }
    for (int i = 0; i < 8; ++i) {
    byte_array8[i] = (large_value >> (i * 8)) & 0xFF;
    }
    
    memcpy(byte_array_mc, &large_value, sizeof(byte_array_mc));

    printf("Bytes in hexadecimal:\n");
    for (int i = 0; i < 8; ++i) {
        printf("%02X ", byte_array8[i]);
    }
    printf("\n");

    // Print the bytes in hexadecimal
    printf("Bytes in hexadecimal:\n");
    for (int i = 0; i < 6; ++i) {
        printf("%02X ", byte_array6[i]);
    }
    printf("\n");


    // Print the bytes in hexadecimal
    printf("Bytes in hexadecimal:\n");
    for (int i = 0; i < 6; ++i) {
        printf("%02X ", byte_array_mc[i]);
    }
    printf("\n");


    return 0;
}

