package com.razorfish.fluent.autotag.aws;

import com.day.cq.tagging.InvalidTagFormatException;
import com.day.cq.tagging.Tag;
import com.day.cq.tagging.TagManager;
import com.day.cq.workflow.WorkflowSession;
import com.day.cq.workflow.exec.WorkItem;
import java.util.List;
import javax.jcr.Node;

import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.day.cq.commons.jcr.JcrConstants;
import com.day.cq.dam.api.Asset;
import com.day.cq.dam.api.DamConstants;
import com.day.cq.dam.commons.process.AbstractAssetWorkflowProcess;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.Emotion;
import com.amazonaws.services.rekognition.model.FaceDetail;


public abstract class AbstractAWSWorkflowStep extends AbstractAssetWorkflowProcess {

	
	/** Default log. */
	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	public static AWSCredentials getCredentials() throws AmazonClientException {
		AWSCredentials credentials;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
            return credentials;
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (/Users/<userid>/.aws/credentials), and is in a valid format.", e);
        }
	}

    
       
	/**
	 * add tag metadata to the actual asset
	 * 
	 * @param workItem
	 * @param wfSession
	 * @param asset
	 * @param tagManager
	 * @param tagArray
	 * @throws Exception
	 * 
	 */
	protected void addMetaData(WorkItem workItem, WorkflowSession wfSession, final Asset asset, TagManager tagManager,
			String[] tagArray) throws Exception {
		final ResourceResolver resolver = getResourceResolver(wfSession.getSession());
		final Resource assetResource = asset.adaptTo(Resource.class);
		final Resource metadata = resolver.getResource(assetResource,
				JcrConstants.JCR_CONTENT + "/" + DamConstants.METADATA_FOLDER);

		if (null != metadata) {
			final Node metadataNode = metadata.adaptTo(Node.class);

			ValueMap properties = metadata.adaptTo(ValueMap.class);

			String[] existing_tags = properties.get("cq:tags", String[].class);
			if (existing_tags != null && existing_tags.length > 0) {
				log.info(existing_tags.length + " existing tags found");
				tagArray = join(existing_tags, tagArray);
			} else {
				log.info("no existing tags found");
			}
			log.info(tagArray.length + " total tags ");

			metadataNode.setProperty("cq:tags", tagArray);

			metadataNode.getSession().save();
			log.info("added or updated tags");
		} else {
			log.warn("execute: failed setting metdata for asset [{}] in workflow [{}], no metdata node found.",
					asset.getPath(), workItem.getId());
		}
	}

	/**
	 * create individual tags if they don't exist yet
	 * 
	 * @param tagManager
	 * @param entities
	 * @return
	 * @throws InvalidTagFormatException
	 */
	protected String[] createTags(TagManager tagManager, List<Label> entities, String namespace,
			String container) throws InvalidTagFormatException {

		String tagArray[] = new String[entities.size()];
		int index = 0;

		for (Label label : entities) {
			generateTag(tagManager, namespace, container, tagArray, index, label.getName());
			index++;

		}
		return tagArray;
	}
	
	protected String[] createFaceTags(TagManager tagManager, List<FaceDetail> entities, String namespace,
			String container) throws InvalidTagFormatException {

		String tagArray[] = new String[entities.size()+4];
		int index = 0;

		for (FaceDetail faceDetail : entities ) {
			
			for (Emotion emotion : faceDetail.getEmotions() ) {
				if (emotion.getConfidence()>75.0) {
					generateTag(tagManager, namespace, container+ "/emotion", tagArray, index, emotion.getType().toString());
					index++;
				}
			}
			
			if (faceDetail.getGender()!=null && faceDetail.getGender().getConfidence()>75.0) {
				generateTag(tagManager, namespace, container , tagArray, index, faceDetail.getGender().getValue());
				index++;
			}
			if (faceDetail.getBeard()!=null && faceDetail.getBeard().getValue() && faceDetail.getBeard().getConfidence()>75.0) {
				generateTag(tagManager, namespace, container, tagArray, index, "Beard");
				index++;
			}
			if (faceDetail.getEyeglasses()!=null && faceDetail.getEyeglasses().getValue() && faceDetail.getEyeglasses().getConfidence()>75.0) {
				generateTag(tagManager, namespace, container, tagArray, index, "Eyeglasses");
				index++;
			}
			if (faceDetail.getMustache()!=null && faceDetail.getMustache().getValue() && faceDetail.getMustache().getConfidence()>75.0) {
				generateTag(tagManager, namespace, container, tagArray, index, "Mustache");
				index++;
			}
			if (faceDetail.getSmile()!=null && faceDetail.getSmile().getValue() && faceDetail.getSmile().getConfidence()>75.0) {
				generateTag(tagManager, namespace, container, tagArray, index, "Smile");
				index++;
			}
			if (faceDetail.getSunglasses()!=null && faceDetail.getSunglasses().getValue() && faceDetail.getSunglasses().getConfidence()>75.0) {
				generateTag(tagManager, namespace, container, tagArray, index, "Sunglasses");
				index++;
			}

		}
		return tagArray;
	}



	private void generateTag(TagManager tagManager, String namespace, String container, String[] tagArray, int index,
			String element) throws InvalidTagFormatException {
		Tag tag;
		log.info("found face " + element );

		tag = tagManager.createTag(
				namespace + container + "/" + element.replaceAll(" ", "_").toLowerCase(),
				element, "Auto detected : " + element, true);
		tagArray[index] = tag.getNamespace().getName() + ":"
				+ tag.getPath().substring(tag.getPath().indexOf(namespace) + namespace.length() + 1);

		log.info(tag.getNamespace().getName() + ":"
				+ tag.getPath().substring(tag.getPath().indexOf(namespace) + namespace.length() + 1));
	}

	public AbstractAWSWorkflowStep() {
		super();
	}

	/**
	 * Join two arrays
	 * 
	 * @param String1
	 * @param String2
	 * @return
	 */
	protected String[] join(String[] String1, String[] String2) {
		String[] allStrings = new String[String1.length + String2.length];

		System.arraycopy(String1, 0, allStrings, 0, String1.length);
		System.arraycopy(String2, 0, allStrings, String1.length, String2.length);

		return allStrings;
	}

}