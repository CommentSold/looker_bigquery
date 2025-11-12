
view: popstore_onboarding_screen_action {
  derived_table: {
    sql: select
            t1.timestamp,
            t1.anonymous_id,
            t1.user_id,
            t2.first_name,
            t3.last_name,
            t3.email,
            t4.phone_number,
            t1.step_name as Last_Onboarding_Step,
            t1.context_campaign_campaign,
            t1.context_campaign_content,
            t1.context_campaign_context_campaign_topic,
            t1.context_campaign_medium,
            t1.context_campaign_source,
            t1.context_campaign_adgroup,
            t1.context_campaign_adgroupid,
            t1.context_campaign_campaignid,
            t1.context_campaign_device,
            t1.context_campaign_network,
            t1.context_campaign_creative,
            t1.context_campaign_matchtype,
            t1.context_campaign_term,
            t1.context_campaign_id,
            t1.context_campaign_shop_identifier,
            t1.context_campaign_referrer,
            t1.context_campaign_placement,
            t1.context_campaign_loc_physical_ms,
            t1.context_campaign_regintent,
            t1.tier,
            t1.reg,
            TO_JSON_STRING(PARSE_JSON(JSON_QUERY(t2.customization, '$.socialLinks')), true) as socialLinks,
            t1.instagram_handle,
            t1.instagram_followers,
            -- t3.instagram_link_in_bio,
            -- t3.instagram_follower_count,
            -- t3.instagram_is_verified,
            t1.tiktok_handle,
            t1.tiktok_followers,
            -- t3.tiktok_link_in_bio,
            -- t3.tiktok_follower_count,
            -- t3.tiktok_is_verified,
            t1.youtube_handle,
            t1.youtube_followers,
            -- t3.youtube_link_in_bio,
            -- t3.youtube_follower_count,
            -- t3.youtube_is_verified
          -- JSON_QUERY_ARRAY(t2.customization, '$.socialLinks') as socialLinks
            -- t1.scene,
            -- t1.element_action_stage,
            -- t1.instagram_url,
            -- t1.tiktok_url,
            -- t1.youtube_url,
            -- t1.follower_count,
            -- t1.follower_details_instagram_follower_count,
            -- t1.follower_details_tiktok_follower_count
        from (
          SELECT
            `timestamp`,
            anonymous_id,
            user_id IS NULL AS is_anonymous,
            user_id,
            step_name,
            phone_number,
            context_campaign_campaign,
            context_campaign_content,
            context_campaign_context_campaign_topic,
            context_campaign_medium,
            context_campaign_source,
            context_campaign_adgroup,
            context_campaign_adgroupid,
            context_campaign_campaignid,
            context_campaign_device,
            context_campaign_network,
            context_campaign_creative,
            context_campaign_matchtype,
            context_campaign_term,
            context_campaign_id,
            context_campaign_shop_identifier,
            context_campaign_referrer,
            context_campaign_placement,
            context_campaign_loc_physical_ms,
            context_campaign_regintent,
            tier,
            reg,
            scene,
            element_action_stage,
            tiktok_handle,
            instagram_handle,
            youtube_handle,
            instagram_followers,
            tiktok_followers,
            youtube_followers,
            instagram_url,
            tiktok_url,
            youtube_url,
            follower_count,
            follower_details_instagram_follower_count,
            follower_details_tiktok_follower_count
          FROM (
            SELECT
              `timestamp`,
              anonymous_id,
              user_id,
              step_name,
              phone_number,
              context_campaign_campaign,
              context_campaign_content,
              context_campaign_context_campaign_topic,
              context_campaign_medium,
              context_campaign_source,
              context_campaign_adgroup,
              context_campaign_adgroupid,
              context_campaign_campaignid,
              context_campaign_device,
              context_campaign_network,
              context_campaign_creative,
              context_campaign_matchtype,
              context_campaign_term,
              context_campaign_id,
              context_campaign_shop_identifier,
              context_campaign_referrer,
              context_campaign_placement,
              context_campaign_loc_physical_ms,
              context_campaign_regintent,
              tier,
              reg,
              scene,
              element_action_stage,
              tiktok_handle,
              instagram_handle,
              youtube_handle,
              instagram_followers,
              tiktok_followers,
              youtube_followers,
              instagram_url,
              tiktok_url,
              youtube_url,
              follower_count,
              follower_details_instagram_follower_count,
              follower_details_tiktok_follower_count,
              ROW_NUMBER() OVER(PARTITION BY anonymous_id ORDER BY `timestamp` DESC) AS rn
            FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
          )
          WHERE rn = 1
        ) t1
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` t2 ON t2.user_id = t1.user_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` t3 ON t3.user_id = t1.user_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_users` t4 ON t4.user_id = t1.user_id
        WHERE (true = true OR t1.is_anonymous = false) ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: phone_number {
    type: string
    sql: ${TABLE}.phone_number ;;
  }

  dimension: last_onboarding_step {
    type: string
    sql: ${TABLE}.Last_Onboarding_Step ;;
  }

  dimension: context_campaign_campaign {
    type: string
    sql: ${TABLE}.context_campaign_campaign ;;
  }

  dimension: context_campaign_content {
    type: string
    sql: ${TABLE}.context_campaign_content ;;
  }

  dimension: context_campaign_context_campaign_topic {
    type: string
    sql: ${TABLE}.context_campaign_context_campaign_topic ;;
  }

  dimension: context_campaign_medium {
    type: string
    sql: ${TABLE}.context_campaign_medium ;;
  }

  dimension: context_campaign_source {
    type: string
    sql: ${TABLE}.context_campaign_source ;;
  }

  dimension: context_campaign_adgroup {
    type: string
    sql: ${TABLE}.context_campaign_adgroup ;;
  }

  dimension: context_campaign_adgroupid {
    type: string
    sql: ${TABLE}.context_campaign_adgroupid ;;
  }

  dimension: context_campaign_campaignid {
    type: string
    sql: ${TABLE}.context_campaign_campaignid ;;
  }

  dimension: context_campaign_device {
    type: string
    sql: ${TABLE}.context_campaign_device ;;
  }

  dimension: context_campaign_network {
    type: string
    sql: ${TABLE}.context_campaign_network ;;
  }

  dimension: context_campaign_creative {
    type: string
    sql: ${TABLE}.context_campaign_creative ;;
  }

  dimension: context_campaign_matchtype {
    type: string
    sql: ${TABLE}.context_campaign_matchtype ;;
  }

  dimension: context_campaign_term {
    type: string
    sql: ${TABLE}.context_campaign_term ;;
  }

  dimension: context_campaign_id {
    type: string
    sql: ${TABLE}.context_campaign_id ;;
  }

  dimension: context_campaign_shop_identifier {
    type: string
    sql: ${TABLE}.context_campaign_shop_identifier ;;
  }

  dimension: context_campaign_referrer {
    type: string
    sql: ${TABLE}.context_campaign_referrer ;;
  }

  dimension: context_campaign_placement {
    type: string
    sql: ${TABLE}.context_campaign_placement ;;
  }

  dimension: context_campaign_loc_physical_ms {
    type: string
    sql: ${TABLE}.context_campaign_loc_physical_ms ;;
  }

  dimension: context_campaign_regintent {
    type: string
    sql: ${TABLE}.context_campaign_regintent ;;
  }

  dimension: tier {
    type: string
    sql: ${TABLE}.tier ;;
  }

  dimension: reg {
    type: string
    sql: ${TABLE}.reg ;;
  }

  dimension: social_links {
    type: string
    sql: ${TABLE}.socialLinks ;;
  }

  dimension: instagram_handle {
    type: string
    sql: ${TABLE}.instagram_handle ;;
  }

  dimension: instagram_followers {
    type: number
    sql: ${TABLE}.instagram_followers ;;
  }

  dimension: tiktok_handle {
    type: string
    sql: ${TABLE}.tiktok_handle ;;
  }

  dimension: tiktok_followers {
    type: number
    sql: ${TABLE}.tiktok_followers ;;
  }

  dimension: youtube_handle {
    type: string
    sql: ${TABLE}.youtube_handle ;;
  }

  dimension: youtube_followers {
    type: number
    sql: ${TABLE}.youtube_followers ;;
  }

  set: detail {
    fields: [
        timestamp_time,
	anonymous_id,
	user_id,
	first_name,
	last_name,
	email,
	phone_number,
	last_onboarding_step,
	context_campaign_campaign,
	context_campaign_content,
	context_campaign_context_campaign_topic,
	context_campaign_medium,
	context_campaign_source,
	context_campaign_adgroup,
	context_campaign_adgroupid,
	context_campaign_campaignid,
	context_campaign_device,
	context_campaign_network,
	context_campaign_creative,
	context_campaign_matchtype,
	context_campaign_term,
	context_campaign_id,
	context_campaign_shop_identifier,
	context_campaign_referrer,
	context_campaign_placement,
	context_campaign_loc_physical_ms,
	context_campaign_regintent,
	tier,
	reg,
	social_links,
	instagram_handle,
	instagram_followers,
	tiktok_handle,
	tiktok_followers,
	youtube_handle,
	youtube_followers
    ]
  }
}
