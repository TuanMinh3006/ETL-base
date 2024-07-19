# ETL-base
## Project này liên quan đến Car. Và chia thành 4 phần: Extract, Transform, Load, Log
## Mục đích:
Trích xuất các thông tin về car từ các file .CSV, .JSON, .XML có sẵn và từ đó chuyển đổi giá của các car. Và lưu vào file .csv
## Yêu cầu
+ pandas
+ glob
+ xml.etree.ElementTree
+ datetime 
## Phần 1: Extract
Mục đích: Trích xuất các thông tin về car từ các file .CSV, .JSON, .XML từ folder datasource

Input: Các file .CSV, .JSON, .XML

Output: DataFrame chứa các thông tin (car_model,year_of_manufacture,price,fuel) của ô tô trong các file nguồn

![image](https://github.com/user-attachments/assets/a78dbc07-5b42-4eed-bc35-8b53628e7d27)


## Phần 2: Transform 
Mục đích: Làm tròn giá trị price của ô tô lên 2 chữ số hàng thập phần

Output: DataFrame đã được làm tròn price

![image](https://github.com/user-attachments/assets/45674e85-13e3-43e3-8418-22095732b00e)


## Phần 3: Load
Mục đích: Lưu thông tin đã được xử lý vào file .CSV 

Input : DataFrame đã được xử lý phần trên

Output: File .CSV có tên transformed_data.csv lưu các thông tin đã được xử lý

![image](https://github.com/user-attachments/assets/f1beef37-0d6d-4e5e-a3fa-65084585767c)


## Phần 4: Log
Mục đích: Ghi lại các quá trình ETL nhằm quản lý thời gian và có thể nhận biết lỗi nếu xảy ra

Thư viện sử dụng: datetime

OutPut:
![image](https://github.com/user-attachments/assets/b9f1f8e5-6db4-453b-bdc3-b46ba9f15bb1)

