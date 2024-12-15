package ai.learning.controller;

import org.springframework.ai.ollama.OllamaChatModel;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class AskController {

    private OllamaChatModel chatModel;

    public AskController(OllamaChatModel chatModel){
        this.chatModel=chatModel;
    }
    @GetMapping("/ai/generate")
    public String generate(@RequestParam(value = "message", defaultValue = "Tell me a joke") String message) {
        return  this.chatModel.call(message).toString();
    }
}