package com.processor;

import com.model.Account;
import com.model.Profile;
import org.springframework.batch.item.ItemProcessor;

import java.util.Objects;

public class ProfileAccountProcessor implements ItemProcessor<Profile, Account> {
    private int counter = 0;
    @Override
    public Account process(Profile profile) throws Exception {
        counter++;
        if (counter == 3 || counter == 2 ) {
            throw new RuntimeException("something goes wrong. Profile: " + profile);
        }

        Account account = new Account();
        account.setId(profile.getId());
        account.setUserEmail(profile.getEmail());
        account.setUserBrand(profile.getBrand());
        account.setCompleted(Objects.nonNull(profile.getId())
                && Objects.nonNull(profile.getEmail())
                && Objects.nonNull(profile.getBrand()));
        return account;
    }
}
