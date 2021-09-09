START /b /wait cmd /C "E:\kafka\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 2 --topic streams-tfidf-input"

START /b /wait cmd /C "E:\kafka\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 2 --topic streams-tfidf-output"

pause
