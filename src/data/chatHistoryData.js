import { getSupabase } from './database.js';

// Function to clear chat history for a user
const clearChatHistory = async (userId) => {
  try {
    const supabase = getSupabase();
    
    // Check if user has chat history
    const { data: existingChat, error: checkError } = await supabase
      .from('ai_chat_history')
      .select('ai_chat_id')
      .eq('user_id', userId)
      .single();

    if (checkError && checkError.code !== 'PGRST116') {
      throw checkError;
    }

    if (existingChat) {
      // Delete existing chat history
      const { error: deleteError } = await supabase
        .from('ai_chat_history')
        .delete()
        .eq('ai_chat_id', existingChat.ai_chat_id);

      if (deleteError) throw deleteError;
    }

    return true;
  } catch (error) {
    console.error('Error clearing chat history:', error);
    throw error;
  }
};

// Function to save chat history
const saveChatHistory = async (userId, conversation, isModification = false) => {
  try {
    const supabase = getSupabase();
    const now = new Date().toISOString();

    // Check if user already has chat history
    const { data: existingChat, error: checkError } = await supabase
      .from('ai_chat_history')
      .select('*')
      .eq('user_id', userId)
      .single();

    if (checkError && checkError.code !== 'PGRST116') {
      throw checkError;
    }

    let result;
    if (existingChat) {
      if (isModification) {
        // Append new conversation to existing history
        const updatedConversation = {
          history: [
            ...(existingChat.conversation.history || []),
            conversation
          ]
        };

        // Update existing chat history with appended conversation
        const { data, error } = await supabase
          .from('ai_chat_history')
          .update({
            conversation: updatedConversation,
            updated_at: now
          })
          .eq('ai_chat_id', existingChat.ai_chat_id)
          .select()
          .single();

        if (error) throw error;
        result = data;
      } else {
        // Clear existing history and save new conversation
        await clearChatHistory(userId);
        
        // Insert new chat history
        const { data, error } = await supabase
          .from('ai_chat_history')
          .insert({
            user_id: userId,
            conversation: {
              history: [conversation]
            },
            updated_at: now
          })
          .select()
          .single();

        if (error) throw error;
        result = data;
      }
    } else {
      // Insert new chat history
      const { data, error } = await supabase
        .from('ai_chat_history')
        .insert({
          user_id: userId,
          conversation: {
            history: [conversation]
          },
          updated_at: now
        })
        .select()
        .single();

      if (error) throw error;
      result = data;
    }

    return result;
  } catch (error) {
    console.error('Error saving chat history:', error);
    throw error;
  }
};

// Function to get recent chat history for a user
const getRecentChatHistory = async (userId, limit = 5) => {
  try {
    const supabase = getSupabase();

    const { data, error } = await supabase
      .from('ai_chat_history')
      .select('*')
      .eq('user_id', userId)
      .order('updated_at', { ascending: false })
      .limit(limit);

    if (error) throw error;
    return data;
  } catch (error) {
    console.error('Error getting chat history:', error);
    throw error;
  }
};

export {
  saveChatHistory,
  getRecentChatHistory,
  clearChatHistory
}; 