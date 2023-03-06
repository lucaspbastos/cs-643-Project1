# CS 643 Project 1
Lucas Bastos  
Professor Manoop Talasila  
Spring 2023

## EC2 Instances setup
First, set up two EC2 instances on AWS Console Home by first going to the EC2 section to "launch instance". Name the car recognition EC2 instance as "Car Recognizer" and the text recognition EC2 instance as "Text Recognizer". Both instances can be set up with "Amazon Linux 2 Kernel 5.10 AMI 2.0.20220912.1 x86_64 HVM gp2" AMI and the "t2.micro" type. Create a .pem login key-pair (I created "bastos-key" with ED25519 type when I created the first EC2 instance). The second EC2 instance will use this same .pem key for SSH (I created a new security group set to my home IP address and opened the SSH, HTTP, HTTPS ports and then used this same security group with the second EC2 instance).

## SQS setup
Then, set up the SQS by going to the SQS section to "create queue". Choose the FIFO type as it best fits the needs for the project, and name it "CarImageIndexQueue.fifo" Leave the configuration and encryption settings to default. Set the send/receive message queue access policies to the ARN of the LabRole IAM.

## Environment setup
Now connect over SSH to both EC2 instances to set up the projects. The instances require the .pem key file created earlier to connect. The public IP will change with each lab restart so the correct login will be in the EC2 instances "connect" section. 

Once connected, using the credentials from AWS Academy, create a file called ~/.aws/credentials on each instance and paste the credentials. On each instance run:

```
sudo yum update

sudo wget https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo

sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo

sudo yum install -y apache-maven git

git clone https://github.com/lucaspbastos/cs-643-Project1.git

cd cs-643-Project1/

mvn install
```

## Run Programs
Now, the project has been installed on both instances and the target/ directory will be made with a .jar file that will run the apps.

On the TextRecognizer instance, run:
```
java -cp target/original-Project1-1.0-SNAPSHOT.jar TextRecognizer.TextRecognizer
```
The TextRecognizer program will now wait for the CarRecognizer instance to process its images and send over the image indexes through the SQS channel.

Finally, on the CarRecognizer instance, run:
```
java -cp target/original-Project1-1.0-SNAPSHOT.jar CarRecognizer.CarRecognizer
```
The CarRecognizer program will pull the 10 images from the njit-cs-643 bucket, run Rekognition to detect images with cars, then send a message through the SQS channel for each image index, and ending with "-1". This will then exit the CarRecognizer program.

As the TextRecognizer receives image indexes, it will begin to process them in parallel as CarRecognizer is still processing other images. The TextRecognizer program will stop waiting for messages once it receives "-1". Once TextRecognizer completes its text recognition, it will create a file under the results/ directory with each line containing an index and its respective detect text. The TextRecognizer program will exit once the file is written.
