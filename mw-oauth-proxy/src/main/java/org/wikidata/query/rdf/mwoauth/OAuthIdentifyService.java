package org.wikidata.query.rdf.mwoauth;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.github.scribejava.core.model.OAuth1AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth10aService;

class OAuthIdentifyService {
    private static final String USERNAME = "username";
    private final OAuth10aService service;
    private final String url;
    private final JWTVerifier verifier;

    OAuthIdentifyService(OAuth10aService service, String indexUrl, String consumerSecret) {
        this.service = service;
        this.url = indexUrl + "?title=Special:OAuth/identify";
        verifier = JWT.require(Algorithm.HMAC256(consumerSecret)).withClaimPresence(USERNAME).build();

    }

    String getUsername(OAuth1AccessToken accessToken) throws InterruptedException, ExecutionException, IOException {
        return identify(accessToken).getClaim(USERNAME).asString();
    }

    DecodedJWT identify(OAuth1AccessToken accessToken) throws InterruptedException, ExecutionException, IOException {
        return execute(new OAuthRequest(Verb.GET, url), accessToken);
    }

    DecodedJWT execute(OAuthRequest request, OAuth1AccessToken accessToken) throws InterruptedException, ExecutionException, IOException {
        service.signRequest(accessToken, request);
        try (Response response = service.execute(request)) {
            if (!response.isSuccessful()) {
                // Probably an html error page
                throw new RuntimeException(response.getBody());
            }
            return verifier.verify(response.getBody());
        }
    }
}
