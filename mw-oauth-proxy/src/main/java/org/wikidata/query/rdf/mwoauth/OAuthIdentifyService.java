package org.wikidata.query.rdf.mwoauth;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.github.scribejava.core.model.OAuth1AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth10aService;

import lombok.SneakyThrows;

public class OAuthIdentifyService {
    private static final String USERNAME = "username";
    private final OAuth10aService service;
    private final String url;
    private final JWTVerifier verifier;

    public OAuthIdentifyService(OAuth10aService service, String indexUrl, String consumerSecret) {
        this.service = service;
        this.url = indexUrl + "?title=Special:OAuth/identify";
        verifier = JWT.require(Algorithm.HMAC256(consumerSecret)).withClaimPresence(USERNAME).build();
    }

    @SneakyThrows
    public Optional<String> getUsername(OAuth1AccessToken accessToken) {
        return identify(accessToken).map(token -> token.getClaim(USERNAME).asString());
    }

    private Optional<DecodedJWT> identify(OAuth1AccessToken accessToken) throws InterruptedException, ExecutionException, IOException {
        return execute(new OAuthRequest(Verb.GET, url), accessToken);
    }

    private Optional<DecodedJWT> execute(OAuthRequest request, OAuth1AccessToken accessToken) throws InterruptedException, ExecutionException, IOException {
        service.signRequest(accessToken, request);
        try (Response response = service.execute(request)) {
            if (!response.isSuccessful()) {
                // Probably an html error page as opposed to a rejected request.
                return Optional.empty();
            }
            try {
                return Optional.of(verifier.verify(response.getBody()));
            } catch (JWTVerificationException e) {
                // response.body probably contains some json that indicates what exactly went wrong,
                // in many cases this probably indicates a token that is now invalid.
                return Optional.empty();
            }
        }
    }
}
