#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#define LINE_SIZE 64

int getUserId(char*);

int main(int argc, char** argv) {
    FILE *fr, *fw;
    char line[LINE_SIZE], *buf;
    int prevUserId = -1, userId = -1, state, threshold, cnt = 0, toContinue = 1;
    
    fr = fopen(argv[1], "r");
    if (fr == NULL)
        perror("");
    fw = fopen(argv[2], "w");
    if (fw == NULL)
        perror("");
    threshold = atoi(argv[3]);

    buf = (char*) malloc(sizeof(char) * LINE_SIZE * threshold);
    buf[0] = '\0';
    state = 0;

    while (toContinue) {
        switch (state) {
            case 0: {
                int b = 0;
                b = fgets(line, LINE_SIZE, fr) == NULL;
                if (b) {
                    state = 3;
                    break;
                }
                userId = getUserId(line);
                prevUserId = userId;
                cnt = 1;
                strcat(buf, line);
                state = 1;
                break;
            }
            case 1: {
                int b = 0;
                b = fgets(line, LINE_SIZE, fr) == NULL;
                if (b) {
                    state = 3;
                    break;
                }
                userId = getUserId(line);

                if (userId == prevUserId) {
                    ++cnt;
                    if (cnt < threshold) {
                        strcat(buf, line);
                    }
                    else if (cnt == threshold){
                        fprintf(fw, "%s", buf);
                        fprintf(fw, "%s", line);
                    }
                    else {
                        fprintf(fw, "%s", line);
                    }
                }
                else {
                    buf[0] = '\0';
                    state = 2;
                }
                break;
            }
            case 2: {
                prevUserId = userId;
                cnt = 1;
                strcat(buf, line);
                state = 1;
                break;
            }
            case 3: {
                prevUserId = userId;
                toContinue = 0;
                break;
            }
            default:
                break;
        }
    }

    fprintf(fw, "\n");
    fclose(fr);
    fclose(fw);
    return 0;
}

int getUserId(char* line) {
    int index = 0, userId;
    char userIdStr[16];

    while (line[index] != '\t')
        ++index;
    strncpy(userIdStr, line, index);
    userId = atoi(userIdStr);
    return userId;
}

