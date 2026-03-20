package com.iisigroup.df.model;

import com.iisigroup.df.mapstruct.BaseMapperConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.mapstruct.Mapper;
import org.mapstruct.factory.Mappers;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SendMailRqDto {
    // 傳遞給誰
    private String to;
    // 主旨
    private String subject;
    // 文字
    private String body;

    @Mapper(
            config = BaseMapperConfig.class
    )
    public interface SendMailMapper {

        SendMailMapper INSTANCE = Mappers.getMapper(SendMailMapper.class);

        SendMailEvent toEvent(SendMailRqDto sendMailRqDto);
    }
}
