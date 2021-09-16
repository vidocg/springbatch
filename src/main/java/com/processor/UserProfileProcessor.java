package com.processor;

import com.model.Profile;
import com.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class UserProfileProcessor implements ItemProcessor<User, Profile> {
    public static final String PREFIX = "PROFILE_";

    public Profile process(User user) {
        Profile profile = new Profile();
        profile.setId(PREFIX + user.getFirstValue());
        profile.setEmail(PREFIX + user.getSecondValue());
        profile.setBrand(PREFIX + user.getThirdValue());
        return profile;
    }
}
