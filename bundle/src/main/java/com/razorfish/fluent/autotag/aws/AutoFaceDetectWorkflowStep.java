package com.razorfish.fluent.autotag.aws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.osgi.framework.Constants;

import com.amazonaws.services.rekognition.model.Image;
import com.day.cq.dam.api.Asset;

import com.day.cq.workflow.WorkflowException;
import com.day.cq.workflow.WorkflowSession;
import com.day.cq.workflow.exec.WorkItem;
import com.day.cq.workflow.metadata.MetaDataMap;
import com.day.cq.tagging.JcrTagManagerFactory;
import com.day.cq.tagging.Tag;
import com.day.cq.tagging.TagManager;

import com.amazonaws.services.rekognition.AmazonRekognitionClient;
import com.amazonaws.services.rekognition.model.DetectFacesRequest;
import com.amazonaws.services.rekognition.model.DetectFacesResult;
import com.amazonaws.services.rekognition.model.Attribute;
import com.amazonaws.services.rekognition.model.FaceDetail;

@Component

@Service

@Properties({
		@Property(name = Constants.SERVICE_DESCRIPTION, value = "AWS face - Automatic face detection and tagging using aws."),
		@Property(name = Constants.SERVICE_VENDOR, value = "Razorfish"),
		@Property(name = "process.face", value = "AWS face - Automatic face detection and tagging using aws") })
public class AutoFaceDetectWorkflowStep extends AbstractAWSWorkflowStep {

	/** Default log. */
	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final String NAMESPACE = "/etc/tags/aws";
	private static final String CONTAINER = "/face";
	@Reference
	JcrTagManagerFactory tmf;

	public void execute(WorkItem workItem, WorkflowSession wfSession, MetaDataMap args) throws WorkflowException {

		try {
			log.info("Autoface aws workflow step in execute method");
			final Asset asset = getAssetFromPayload(workItem, wfSession.getSession());

			// create tag manager
			TagManager tagManager = getResourceResolver(wfSession.getSession()).adaptTo(TagManager.class);
			Tag superTag = tagManager.resolve(NAMESPACE + CONTAINER);
			Tag tag = null;

			if (superTag == null) {
				tag = tagManager.createTag(NAMESPACE + CONTAINER, "faces", "autodetected faces", true);
				log.info("Tag Name Space created : ", tag.getPath());
			} else {
				tag = superTag;
			}

			byte[] data = new byte[(int) asset.getOriginal().getSize()];
			int numbytesread = asset.getOriginal().getStream().read(data);
			log.debug("Read :  {} of {}", numbytesread, asset.getOriginal().getSize());

			DetectFacesRequest request = new DetectFacesRequest()
					.withImage(new Image().withBytes(ByteBuffer.wrap(data))).withAttributes(Attribute.ALL);

			AmazonRekognitionClient rekognitionClient = new AmazonRekognitionClient(getCredentials());
			rekognitionClient.setSignerRegionOverride("us-east-1");

			DetectFacesResult result = rekognitionClient.detectFaces(request);

			List<FaceDetail> faceDetails = result.getFaceDetails();

			String[] tagArray = createFaceTags(tagManager, faceDetails, NAMESPACE, CONTAINER);

			addMetaData(workItem, wfSession, asset, tagManager, tagArray);

		}

		catch (Exception e) {
			log.error("Error in execution" + e);
			e.printStackTrace();
		}
	}

}