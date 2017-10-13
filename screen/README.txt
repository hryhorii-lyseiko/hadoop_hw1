RUN
hadoop fs -put /root/lw/inputtxt/ hw1input1
hadoop jar hw1.jar AppDriver hw1input1 output

READ RESULT
hadoop fs -cat /root/lw/outputs/output1/output > /root/lw/outputs/output1/output.txt
-cat /root/lw/outputs/output1/output.txt
