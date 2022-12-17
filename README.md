# Nhóm 59: Tìm hiểu Apache Hadoop và viết ứng dụng demo
- Tô Thanh Phong            19110050
- Tôn Thiên Thạch           19110455
1. Tạo Cluster trên AWS EMR
* Chọn dịch vụ Amazon EMR > chọn Create cluster
![image](https://user-images.githubusercontent.com/69313033/208219009-99304f6f-41f5-4baa-97b0-45f2a0fc37a4.png)
* Chọn các thành phần cần sử dụng trong hệ thống Hadoop > Next
![image](https://user-images.githubusercontent.com/69313033/208219263-a505ee58-92ce-4dac-94b2-a8b2392c8f86.png)
* Chọn số lượng Node, TaskNode cấu hình cho Hadoop
* Bỏ tick Auto-termination
* Chọn Next
![image](https://user-images.githubusercontent.com/69313033/208220048-513018e1-75c1-4975-a315-1cb187bf6136.png)
* Đặt tên cho Cluster
* Tick chọn Termination protection
* Chọn Next
![image](https://user-images.githubusercontent.com/69313033/208220356-7436952a-e603-4992-a44c-11b6350c9461.png)
* Chọn EC2 key pair để kết nối với máy master
* Chọn Create cluster
![image](https://user-images.githubusercontent.com/69313033/208220485-678aec5f-6e2f-4df1-916b-d43df1bf4fe5.png)
* Sau khi tạo sẽ ra màn hình quản lý Cluster vừa tạo
![image](https://user-images.githubusercontent.com/69313033/208220537-6bc55b6f-ab48-44c6-9cf5-480f0c77c18c.png)
* Chọn dịch vụ EC2 > tìm máy ảo master > Connect
![image](https://user-images.githubusercontent.com/69313033/208220895-ee308242-8c34-4a23-8fa3-e88595c355a3.png)
* Màn hình Console giúp tương tác với máy ảo master và chạy các ứng dụng phân tán 
![image](https://user-images.githubusercontent.com/69313033/208221032-7ef8550b-02dc-4ab9-aa45-14696fd19780.png)
