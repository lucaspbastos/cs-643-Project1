package TextRecognizer;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.TextDetection;
import com.amazonaws.services.rekognition.model.DetectTextRequest;
import com.amazonaws.services.rekognition.model.DetectTextResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.IOUtils;
import java.time.LocalDateTime;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TextRecognizer {
    public static void main(String[] args) {
        // Set bucket name and SQS name
        String bucketName = "njit-cs-643";
        String sqsName = "CarImageIndexQueue.fifo";

        System.out.println("Started.");
        textRecognizerPipeline(bucketName, sqsName);
        System.out.println("Finished.");
    }

    private static void textRecognizerPipeline(String bucketName, String sqsName) {
        LocalDateTime currentDateTime = LocalDateTime.now();
        Boolean run = true;

        try {
            // Create file to write results to
            File result = new File("results/run-" + currentDateTime);
            FileWriter writeResult = new FileWriter(result);

            // Loop until end of sequence
            while (run) {
                List<Message> messagesList = receiveMessagesFromSQS(sqsName);
                // If there are messages, process them
                if (messagesList.size() > 0) {
                    for (Message message : messagesList) {
                        String index = message.getBody();
                        // If end of sequence, close file and break from loop
                        if (index.compareTo("-1") == 0) {
                            System.out.println("End of sequence. File written to " + result.toString());
                            run = false;
                            writeResult.close();
                            break;
                        }
                        // Get image from S3 bucket
                        ByteBuffer imageBytes = getImageFromS3Bucket(bucketName, index);
                        // Get text from image
                        String detectedText = getDetectedText(imageBytes);
                        // If text detected, write to file
                        if (detectedText.length() > 0) {
                            writeResult.write(index + ": " + detectedText + "\n");
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private static ByteBuffer getImageFromS3Bucket(String bucketName, String index) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion("us-east-1").build();
        S3ObjectInputStream s3inputStream = null;
        ByteBuffer returnBuffer = ByteBuffer.allocate(1024);

        try {
            // Get image from S3 bucket
            S3Object object = s3.getObject(bucketName, index + ".jpg");

            // Convert input stream to ByteBuffer
            s3inputStream = object.getObjectContent();
            System.out.println("S3: Downloaded " + index + ".jpg");
            returnBuffer = ByteBuffer.wrap(IOUtils.toByteArray(s3inputStream));
        } catch (AmazonServiceException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            // Close input stream
            try {
                if (s3inputStream != null) {
                    s3inputStream.close();
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
        return returnBuffer;
    }

    private static String getDetectedText(ByteBuffer imageBytes) {
        String detectedText = "";
        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
        try {
            // Request Rekognition to detect text
            DetectTextRequest request = new DetectTextRequest()
                    .withImage(new Image()
                            .withBytes(imageBytes));

            DetectTextResult result = rekognitionClient.detectText(request);
            List<TextDetection> texts = result.getTextDetections();

            // Iterate through texts, get first detected (full text)
            for (TextDetection text : texts) {
                System.out.println("Rekognition: text detected " + text.getDetectedText());
                detectedText = text.getDetectedText();
                break;
            }
        } catch (AmazonRekognitionException e) {
            e.printStackTrace();
        }
        return detectedText;
    }

    private static List<Message> receiveMessagesFromSQS(String queueURL) {
        final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        String queueUrl = sqs.getQueueUrl(queueURL).getQueueUrl();

        // Receive and save messages from the queue
        List<Message> messagesList = sqs.receiveMessage(queueUrl).getMessages();
        // Delete messages in queue
        for (Message m : messagesList) {
            System.out.println("SQS: received " + m.getBody());
            sqs.deleteMessage(queueUrl, m.getReceiptHandle());
        }
        return messagesList;
    }
}
