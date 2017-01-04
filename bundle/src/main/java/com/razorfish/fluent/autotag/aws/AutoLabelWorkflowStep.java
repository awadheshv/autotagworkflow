package com.razorfish.fluent.autotag.aws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.nio.ByteBuffer;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.osgi.framework.Constants;

import com.amazonaws.services.rekognition.AmazonRekognitionClient;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.day.cq.dam.api.Asset;

import com.day.cq.workflow.WorkflowException;
import com.day.cq.workflow.WorkflowSession;
import com.day.cq.workflow.exec.WorkItem;
import com.day.cq.workflow.metadata.MetaDataMap;
import com.day.cq.tagging.JcrTagManagerFactory;
import com.day.cq.tagging.Tag;
import com.day.cq.tagging.TagManager;


@Component

@Service

@Properties({
		@Property(name = Constants.SERVICE_DESCRIPTION, value = "AWS label - Automatic label detection and tagging using aws."),
		@Property(name = Constants.SERVICE_VENDOR, value = "Razorfish"),
		@Property(name = "process.label", value = "AWS label - Automatic label detection and tagging using aws") })
public class AutoLabelWorkflowStep extends AbstractAWSWorkflowStep {

	/** Default log. */
	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final String NAMESPACE = "/etc/tags/aws";
	private static final String CONTAINER = "/label";
	private static final int MAX_LABELS = 10;
	@Reference
	JcrTagManagerFactory tmf;

	public void execute(WorkItem workItem, WorkflowSession wfSession, MetaDataMap args) throws WorkflowException {

		try {
			log.info("Autolabel aws workflow step in execute method");
			final Asset asset = getAssetFromPayload(workItem, wfSession.getSession());

			// create tag manager
			TagManager tagManager = tmf.getTagManager(wfSession.getSession());
			Tag superTag = tagManager.resolve(NAMESPACE + CONTAINER);
			Tag tag = null;

			if (superTag == null) {
				tag = tagManager.createTag(NAMESPACE + CONTAINER, "labels", "autodetected labels", true);
				log.info("Tag Name Space created : ", tag.getPath());
			} else {
				tag = superTag;
			}

			byte[] data = new byte[(int) asset.getOriginal().getSize()];
			int numbytesread = asset.getOriginal().getStream().read(data);
			log.info("Read : {} of {}", numbytesread, asset.getOriginal().getSize());

			DetectLabelsRequest request = new DetectLabelsRequest()
	                .withImage(new Image().withBytes(ByteBuffer.wrap(data)))
	                .withMaxLabels(MAX_LABELS)
	                .withMinConfidence(77F);

	        AmazonRekognitionClient rekognitionClient = new AmazonRekognitionClient(getCredentials());
	        rekognitionClient.setSignerRegionOverride("us-east-1");
	
	        DetectLabelsResult result = rekognitionClient.detectLabels(request);
	            
	        List<Label> labels = result.getLabels();
	        
	        String[] tagArray = createTags(tagManager, labels, NAMESPACE, CONTAINER);

			addMetaData(workItem, wfSession, asset, tagManager, tagArray);
	

		}

		catch (Exception e) {
			log.error("Error in execution" + e);
			e.printStackTrace();
		}
	}

}