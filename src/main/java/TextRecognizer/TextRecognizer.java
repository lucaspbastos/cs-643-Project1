/*  Lucas Bastos
    CS 643-852
    Professor Manoop Talasila
    Spring 2023
*/
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
import java.util.ArrayList;

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
                        // Get texts from image
                        List<String> detectedTexts = getDetectedText(imageBytes);
                        // If texts detected, write to file
                        if (detectedTexts.size() > 0) {
                            writeResult.write(
                                    index + ": " + detectedTexts.toString().replace("[", "").replace("]", "") + "\n");
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
            S3Object img = s3.getObject(bucketName, index + ".jpg");

            // Convert input stream to ByteBuffer
            s3inputStream = img.getObjectContent();
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

    private static List<String> getDetectedText(ByteBuffer imageBytes) {
        List<String> detectedTexts = new ArrayList<String>();
        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
        try {
            // Request Rekognition to detect text
            DetectTextRequest request = new DetectTextRequest()
                    .withImage(new Image()
                            .withBytes(imageBytes));

            DetectTextResult result = rekognitionClient.detectText(request);
            List<TextDetection> texts = result.getTextDetections();

            // Iterate through texts and add to list
            for (TextDetection text : texts) {
                System.out.println("Rekognition: text detected " + text.getDetectedText());
                addNonDuplicateEntry(detectedTexts, text.getDetectedText());
            }
        } catch (AmazonRekognitionException e) {
            e.printStackTrace();
        }
        // Filter out duplicate texts and return
        return detectedTexts;
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

    private static void addNonDuplicateEntry(List<String> list, String entry) {
        // If similar or exact duplicate, do not add to list
        for (String s : list) {
            if (s.contains(entry)) {
                System.out.println("File: duplicate detected " + entry);
                return;
            }
        }
        System.out.println("File: added " + entry);
        list.add(entry);
    }
}
