/*  Lucas Bastos
    CS 643-852
    Professor Manoop Talasila
    Spring 2023
*/

package CarRecognizer;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.IOUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class CarRecognizer {
    public static void main(String[] args) {
        // Set bucket name, SQS name, and indexes/file names
        String bucketName = "njit-cs-643";
        String sqsName = "CarImageIndexQueue.fifo";

        carRecognizerPipeline(bucketName, sqsName);
    }

    private static void carRecognizerPipeline(String bucketName, String sqsName) {
        // Read images from S3 -> send each to Rekognition -> for each, if car detected,
        // send index as SQS message
        System.out.println("Started.");
        List<String> indexes = getIndexesFromS3Bucket(bucketName);
        for (String index : indexes) {
            // Get image from S3 bucket
            ByteBuffer imageBytes = getImageFromS3Bucket(bucketName, index);
            // Send image to Rekognition for detecting cars
            Boolean hasCar = carDetected(imageBytes);
            if (hasCar) {
                // Send index to SQS
                sendMessageToSQS(sqsName, index);
            }
        }
        // Send -1 to SQS to indicate end of pipeline
        sendMessageToSQS(sqsName, "-1");
        System.out.println("Finished.");
    }

    private static List<String> getIndexesFromS3Bucket(String bucketName) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion("us-east-1").build();
        List<String> indexes = new ArrayList<String>();
        try {
            // Get list of objects in bucket
            indexes = s3.listObjects(bucketName).getObjectSummaries().stream().map(os -> os.getKey())
                    .collect(Collectors.toList());

        } catch (AmazonServiceException e) {
            System.out.println(e.getMessage());
        }
        return indexes;
    }

    private static ByteBuffer getImageFromS3Bucket(String bucketName, String index) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion("us-east-1").build();
        S3ObjectInputStream s3inputStream = null;
        ByteBuffer returnBuffer = ByteBuffer.allocate(1024);

        try {
            // Get image from S3 bucket
            S3Object object = s3.getObject(bucketName, index);

            // Convert input stream to ByteBuffer
            s3inputStream = object.getObjectContent();
            System.out.println("S3: Downloaded " + index);
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

    private static Boolean carDetected(ByteBuffer imageBytes) {
        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
        try {
            // Request Rekognition to detect labels
            DetectLabelsRequest request = new DetectLabelsRequest()
                    .withImage(new Image()
                            .withBytes(imageBytes))
                    .withMinConfidence(90F);

            DetectLabelsResult result = rekognitionClient.detectLabels(request);
            List<Label> texts = result.getLabels();

            // Iterate through labels, return true if "Car"
            for (Label text : texts) {
                if (text.getName().compareTo("Car") == 0) {
                    System.out
                            .println("Rekognition: Detected " + text.getName() + " " + text.getConfidence() + "%");
                    return true;
                }
            }
        } catch (AmazonRekognitionException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static void sendMessageToSQS(String queueURL, String message) {
        final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        String queueUrl = sqs.getQueueUrl(queueURL).getQueueUrl();
        // Generate a UUID for the message deduplication ID which is required for FIFO
        UUID uuid = UUID.randomUUID();

        // Create message request
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(message)
                .withMessageDeduplicationId("car-group-" + uuid)
                .withMessageGroupId("car-group");

        // Send message
        System.out.println("SQS: sending \"" + message + "\"");
        sqs.sendMessage(send_msg_request);
    }
}
